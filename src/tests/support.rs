//! Test support helpers.

use crate::Config;
use tempfile::TempDir;

/// Create a temporary directory under /tmp/storage_test/<name>.
///
/// The directory is created if it doesn't exist and returned as a TempDir
/// that will be cleaned up when dropped.
pub fn test_dir(name: &str) -> TempDir {
    let path = std::env::temp_dir().join(format!("storage_test_{name}"));
    let _ = std::fs::remove_dir_all(&path); // Start clean
    std::fs::create_dir_all(&path).unwrap();
    TempDir::with_prefix_in(name, &path).unwrap()
}

/// Create a test config with defaults suitable for smoke/fuzz tests.
///
/// Creates one temp directory per shard slot.
/// - M = disk_failures, K = M+1 data shards, C = M parity shards, N = 2M+1 total shards
/// - metadata_replicas = custom
/// - disk_uuids: empty means "generate on startup"
pub fn make_test_config(tmp: &TempDir, disk_failures: u32, metadata_replicas: usize) -> Config {
    let n = disk_failures * 2 + 1;
    let disk_paths: Vec<String> = (0..n)
        .map(|i| tmp.path().join(format!("disk_{i}")))
        .into_iter()
        .map(|p| p.display().to_string())
        .collect();
    Config {
        disk_paths,
        disk_failures,
        metadata_replicas,
        disk_uuids: Vec::new(),
        compression_level: None,
    }
}
