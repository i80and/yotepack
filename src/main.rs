#![forbid(unsafe_code)]

use clap::Parser;
use erasure_s3_storage::{Config, ObjectStorage};

#[derive(Parser, Debug)]
#[command(name = "erasure-s3-storage", about = "Erasure-coded S3-style object storage server")]
struct Cli {
    /// Disk paths (one per shard, required). Total = disk_failures*2 + 1.
    #[arg(short, long, num_args = 1..)]
    disk: Vec<String>,

    /// Number of tolerable disk failures
    #[arg(short, long, default_value = "1")]
    failures: u32,

    /// Chunk size in bytes (default: 67108864 = 64 MiB)
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    chunk_size: usize,

    /// Port to listen on (for future HTTP server)
    #[arg(short, long, default_value_t = 8080)]
    port: u16,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();
    let expected_shards = cli.failures * 2 + 1;
    if cli.disk.len() != expected_shards as usize {
        eprintln!(
            "Error: --disk requires {} paths (for M={} failures), got {}",
            expected_shards, cli.failures, cli.disk.len()
        );
        std::process::exit(1);
    }

    let config = Config {
        disk_paths: cli.disk,
        disk_failures: cli.failures,
        chunk_size: cli.chunk_size,
        metadata_replicas: 0, // 0 = all disks
        disk_uuids: Vec::new(),
    };

    tracing::info!(
        "Starting erasure-coded storage server with config: M={}, shards={}, chunks={}",
        config.disk_failures,
        config.total_shards(),
        config.chunk_size
    );

    // Create storage instance
    let storage = match ObjectStorage::new(config.clone()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to create storage: {e}");
            std::process::exit(1);
        }
    };

    // Recover on startup (pending versions, GC)
    if let Err(e) = storage.recover_on_startup() {
        tracing::warn!("Startup recovery had warnings: {e}");
    }

    // For now, just keep the server running
    // TODO: Add HTTP/gRPC server for S3-compatible API
    tracing::info!("Server started, listening on port {}", cli.port);

    // Signal handler for graceful shutdown
    let (tx, rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        if let Err(_) = tokio::signal::ctrl_c().await {
            let _ = tx.send(());
        }
    });

    let _ = rx.await;
    tracing::info!("Shutting down...");
}

#[cfg(test)]
mod tests {
    use erasure_s3_storage::{Config, ObjectStorage};
    use tempfile::TempDir;

    fn make_test_config(tmp: &TempDir) -> Config {
        let n = 3; // M=1: K=2 data shards + C=1 parity shard = 3 total
        let disk_paths: Vec<String> = (0..n)
            .map(|i| tmp.path().join(format!("disk_{i}")))
            .into_iter()
            .map(|p| p.display().to_string())
            .collect();
        Config {
            disk_paths,
            disk_failures: 1,
            chunk_size: 1024, // 1 KiB for fast tests
            metadata_replicas: 0, // 0 = all disks
            disk_uuids: Vec::new(),
        }
    }

    #[tokio::test]
    async fn test_put_get() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();

        let data = b"Hello, erasure-coded world!";
        let token = storage.put("test-key", data).await.unwrap();
        let result = storage.get("test-key", Some(token)).await.unwrap();

        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_put_get_large() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();

        // Create data larger than chunk size to trigger multiple chunks
        let data: Vec<u8> = (0..10240).map(|i| (i % 256) as u8).collect();
        let token = storage.put("large-key", &data).await.unwrap();
        let result = storage.get("large-key", Some(token)).await.unwrap();

        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_read_committed() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();

        // Write first version
        let token1 = storage.put("key", b"version1").await.unwrap();

        // Read without token (read-committed)
        let result = storage.get("key", None).await.unwrap();
        assert_eq!(result, b"version1");

        // Write second version
        let _token2 = storage.put("key", b"version2").await.unwrap();

        // Read without token should see latest
        let result = storage.get("key", None).await.unwrap();
        assert_eq!(result, b"version2");

        // Read with first token should see first version
        let result = storage.get("key", Some(token1)).await.unwrap();
        assert_eq!(result, b"version1");
    }

    #[tokio::test]
    async fn test_delete() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();

        storage.put("key", b"data").await.unwrap();
        storage.delete("key").unwrap();

        // After delete, reading should fail
        let result = storage.get("key", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();

        storage.put("obj1", b"data1").await.unwrap();
        storage.put("obj2", b"data2").await.unwrap();
        storage.put("other", b"data3").await.unwrap();

        let entries = storage.list("", None, 100).unwrap();
        // Should only show committed objects with data
        assert!(entries.len() >= 2);
    }

    #[tokio::test]
    async fn test_erasure_coding() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();

        // Test with data that spans multiple chunks
        let data: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        let token = storage.put("multi-chunk", &data).await.unwrap();
        let result = storage.get("multi-chunk", Some(token)).await.unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_set_get_metadata() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();

        storage.put("key", b"data").await.unwrap();

        // Set metadata
        storage
            .set_metadata_value("key", "content-type", "text/plain")
            .unwrap();

        // Get metadata back
        let value = storage.get_metadata_value("key", "content-type").unwrap();
        assert_eq!(value, Some("text/plain".to_string()));

        // Verify the data is still readable
        let result = storage.get("key", None).await.unwrap();
        assert_eq!(result, b"data");
    }

    #[tokio::test]
    async fn test_metadata_content_type() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();

        storage.put("img.png", b"image data").await.unwrap();

        // Set content type
        storage.set_content_type("img.png", "image/png").unwrap();

        // Get content type back
        let ct = storage.get_content_type("img.png").unwrap();
        assert_eq!(ct, Some("image/png".to_string()));

        // Non-existent key should return None
        let ct2 = storage.get_content_type("nonexistent").unwrap();
        assert_eq!(ct2, None);
    }

    #[tokio::test]
    async fn test_metadata_cache_control() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();

        storage.put("file.txt", b"text data").await.unwrap();

        // Set cache control
        storage
            .set_cache_control("file.txt", "max-age=3600")
            .unwrap();

        // Get cache control back
        let cc = storage.get_cache_control("file.txt").unwrap();
        assert_eq!(cc, Some("max-age=3600".to_string()));
    }

    #[tokio::test]
    async fn test_metadata_acl() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();

        storage.put("private.txt", b"secret").await.unwrap();

        // Set ACL (private)
        let acl = serde_json::json!({ "grants": [{"user": "admin", "permission": "WRITE"}]});
        storage.set_acl("private.txt", &acl).unwrap();

        // Get ACL back
        let retrieved_acl = storage.get_acl("private.txt").unwrap();
        assert!(retrieved_acl.is_some());
        assert_eq!(retrieved_acl.unwrap(), acl);

        // Non-existent key should return None
        let acl2 = storage.get_acl("nonexistent").unwrap();
        assert_eq!(acl2, None);
    }

    #[tokio::test]
    async fn test_metadata_case_insensitive() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();

        storage.put("key", b"data").await.unwrap();

        // Set metadata with mixed case
        storage
            .set_metadata_value("key", "Content-Type", "application/json")
            .unwrap();

        // Should be retrievable with any case
        let val1 = storage.get_metadata_value("key", "content-type").unwrap();
        assert_eq!(val1, Some("application/json".to_string()));

        let val2 = storage.get_metadata_value("key", "CONTENT-TYPE").unwrap();
        assert_eq!(val2, Some("application/json".to_string()));
    }

    #[tokio::test]
    async fn test_metadata_persists_with_data() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();

        storage.put("key", b"data").await.unwrap();
        storage
            .set_metadata_value("key", "custom", "value123")
            .unwrap();

        // Data should still be readable
        let result = storage.get("key", None).await.unwrap();
        assert_eq!(result, b"data");

        // Metadata should be intact
        let value = storage.get_metadata_value("key", "custom").unwrap();
        assert_eq!(value, Some("value123".to_string()));

        // List should still work (check for any non-empty entry)
        let entries = storage.list("", None, 100).unwrap();
        assert!(!entries.is_empty());
    }
}
