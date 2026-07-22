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
/// - M=1 (2 shards + 1 parity = 3 disks)
/// - chunk_size=1 KiB (fast tests)
/// - metadata_replicas=0 (all disks)
/// - disk_uuids: empty means "generate on startup"
pub fn make_test_config(tmp: &TempDir, disk_failures: u32, chunk_size: usize, metadata_replicas: usize) -> Config {
    Config {
        db_path: format!("{}/db", tmp.path().display()),
        disk_base: format!("{}/disks", tmp.path().display()),
        disk_failures,
        chunk_size,
        metadata_replicas,
        disk_uuids: Vec::new(),
    }
}
