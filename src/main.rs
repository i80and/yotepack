#![forbid(unsafe_code)]

use clap::Parser;
use erasure_s3_storage::{Config, ObjectStorage};

#[derive(Parser, Debug)]
#[command(name = "erasure-s3-storage", about = "Erasure-coded S3-style object storage server")]
struct Cli {
    /// Base directory for storage
    #[arg(short, long, default_value = "./storage")]
    storage: String,

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
    let base = cli.storage;

    let config = Config {
        db_path: format!("{base}/fjall_db"),
        disk_base: format!("{base}/disks"),
        disk_failures: cli.failures,
        chunk_size: cli.chunk_size,
        metadata_replicas: 0, // 0 = all disks
    };

    tracing::info!(
        "Starting erasure-coded storage server with config: M={}, chunks={}, total_shards={}",
        config.disk_failures,
        config.chunk_size,
        config.total_shards()
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
        Config {
            db_path: format!("{}/db", tmp.path().display()),
            disk_base: format!("{}/disks", tmp.path().display()),
            disk_failures: 1,
            chunk_size: 1024, // 1 KiB for fast tests
            metadata_replicas: 0, // 0 = all disks
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
}
