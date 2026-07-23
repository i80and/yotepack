/// Cluster ID management: each disk carries a UUID identifying the pool.
use std::fs;
use std::path::Path;

use crate::errors::{StorageError, StorageResult};

/// Filename used to store the cluster ID on each disk.
pub(crate) const CLUSTER_ID_FILENAME: &str = ".cluster_id";

/// A cluster ID is simply a UUID stored on each disk.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClusterId(pub uuid::Uuid);

impl ClusterId {
    /// Generate a new v4 cluster ID.
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// Read the cluster ID from a disk directory.
    /// Returns `Ok(None)` if the file does not exist.
    pub fn read_from(disk_path: &Path) -> StorageResult<Option<Self>> {
        let path = disk_path.join(CLUSTER_ID_FILENAME);
        match fs::read_to_string(&path) {
            Ok(contents) => {
                let id = uuid::Uuid::parse_str(contents.trim())
                    .map_err(|e| StorageError::Transient(format!(
                        "invalid cluster ID on {}: {e}",
                        disk_path.display()
                    )))?;
                Ok(Some(Self(id)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Transient(format!(
                "failed to read cluster ID from {}: {e}",
                disk_path.display()
            ))),
        }
    }

    /// Write this cluster ID to a disk directory.
    /// Creates the directory if it doesn't exist.
    pub fn write_to(&self, disk_path: &Path) -> StorageResult<()> {
        fs::create_dir_all(disk_path).map_err(|e| {
            StorageError::Transient(format!("failed to create disk dir: {e}"))
        })?;
        let path = disk_path.join(CLUSTER_ID_FILENAME);
        fs::write(&path, format!("{}\n", self.0))
            .map_err(|e| StorageError::Transient(format!(
                "failed to write cluster ID to {}: {e}",
                disk_path.display()
            )))
    }

    /// Validate that the disk's cluster ID matches the expected one.
    /// Returns an error if the IDs don't match.
    pub fn validate(&self, expected: &Option<Self>, disk_index: usize) -> StorageResult<()> {
        let actual_id = self.0;
        match expected {
            None => {
                // No expected ID — accept whatever is on disk (legacy compat or fresh disk).
                // The caller should generate a UUID for this disk if it's new.
                Ok(())
            }
            Some(expected_id) => {
                if actual_id == expected_id.0 {
                    Ok(())
                } else {
                    Err(StorageError::ClusterIdMismatch {
                        disk_index,
                        expected: expected_id.0.to_string(),
                        actual: actual_id.to_string(),
                    })
                }
            }
        }
    }
}

impl std::fmt::Display for ClusterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Scan a disk path for a cluster ID, returning a new one if missing.
/// This is called during initialization to handle both existing and fresh disks.
pub(crate) fn scan_disk_cluster_id(
    disk_path: &Path,
    expected: &Option<ClusterId>,
    disk_index: usize,
) -> StorageResult<ClusterId> {
    match ClusterId::read_from(disk_path)? {
        Some(actual) => {
            // Validate against config
            match expected {
                Some(expected_id) if actual != *expected_id => {
                    return Err(StorageError::ClusterIdMismatch {
                        disk_index,
                        expected: expected_id.0.to_string(),
                        actual: actual.0.to_string(),
                    });
                }
                _ => {}
            }
            Ok(actual)
        }
        None => {
            // No cluster ID on disk — it's a fresh disk.
            match expected {
                Some(expected_id) => {
                    // Config expects a UUID but disk is empty.
                    // This could be a replaced disk or a mis-placed disk.
                    // For now, write the expected UUID to the disk.
                    // This handles the case of replacing a failed disk.
                    expected_id.write_to(disk_path)?;
                    Ok(expected_id.clone())
                }
                None => {
                    // Neither config nor disk has an ID. Generate a new one.
                    let new_id = ClusterId::generate();
                    new_id.write_to(disk_path)?;
                    Ok(new_id)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_id_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let id = ClusterId::generate();
        id.write_to(tmp.path()).unwrap();
        let read = ClusterId::read_from(tmp.path()).unwrap().unwrap();
        assert_eq!(id, read);
    }

    #[test]
    fn test_cluster_id_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let result = ClusterId::read_from(tmp.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_scan_disk_generates_on_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let disk_path = tmp.path().join("disk_0");
        fs::create_dir_all(&disk_path).unwrap();

        // Scan with no expected ID — should generate
        let id = scan_disk_cluster_id(&disk_path, &None, 0).unwrap();
        let read = ClusterId::read_from(&disk_path).unwrap().unwrap();
        assert_eq!(id, read);
    }

    #[test]
    fn test_scan_disk_accepts_existing() {
        let tmp = tempfile::tempdir().unwrap();
        let disk_path = tmp.path().join("disk_0");
        fs::create_dir_all(&disk_path).unwrap();

        let existing = ClusterId::generate();
        existing.write_to(&disk_path).unwrap();

        // Scan with matching expected — should accept
        let result = scan_disk_cluster_id(&disk_path, &Some(existing.clone()), 0).unwrap();
        assert_eq!(result, existing);
    }

    #[test]
    fn test_scan_disk_rejects_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let disk_path = tmp.path().join("disk_0");
        fs::create_dir_all(&disk_path).unwrap();

        let on_disk = ClusterId::generate();
        on_disk.write_to(&disk_path).unwrap();

        let expected = ClusterId::generate();
        let result = scan_disk_cluster_id(&disk_path, &Some(expected), 0);
        assert!(result.is_err());
    }
}
