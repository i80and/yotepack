/// Disk management and chunk storage operations.
use crate::config::Config;
use crate::erasure::ErasureCoder;
use crate::errors::{StorageError, StorageResult};
use crate::checksum;

/// Status of a version in the KV store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VersionStatus {
    Committed = 0x00,
    Pending = 0x01,
    Deleted = 0x02,
}

/// Reconstructed data plus a list of (shard_index, shard_data) for committed shards.
type ReconstructResult = (Vec<u8>, Vec<(usize, Vec<u8>)>);

impl VersionStatus {
    pub fn from_u8(value: u8) -> StorageResult<Self> {
        match value {
            0x00 => Ok(VersionStatus::Committed),
            0x01 => Ok(VersionStatus::Pending),
            0x02 => Ok(VersionStatus::Deleted),
            _ => Err(StorageError::Transient(format!(
                "unknown version status: {value}"
            ))),
        }
    }

    pub fn to_u8(&self) -> u8 {
        *self as u8
    }
}

/// Metadata for a single object version.
#[derive(Debug, Clone)]
pub struct VersionMeta {
    pub version: u64,
    pub chunk_ids: Vec<String>,
    /// Per-chunk xxHash3-128 checksums (of chunk data before erasure encoding).
    pub chunk_checksums: Vec<u128>,
    pub checksum: u128,
    pub status: VersionStatus,
    pub data_size: usize,
    /// Arbitrary user-defined metadata (e.g., Content-Type, ACL, custom headers).
    /// Keys are lowercase; values may be any string.
    pub metadata: std::collections::HashMap<String, String>,
}

/// A single physical disk.
#[derive(Debug)]
pub struct Disk {
    pub path: std::path::PathBuf,
    pub cluster_id: crate::cluster::ClusterId,
    is_failed: std::sync::Mutex<bool>,
}

impl Clone for Disk {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            cluster_id: self.cluster_id.clone(),
            is_failed: std::sync::Mutex::new(*self.is_failed.lock().unwrap()),
        }
    }
}

impl Disk {
    /// Create a new disk at the given path.
    pub fn new(path: std::path::PathBuf, cluster_id: crate::cluster::ClusterId) -> Self {
        Self {
            path,
            cluster_id,
            is_failed: std::sync::Mutex::new(false),
        }
    }

    /// Returns the path to the shards directory for this disk.
    pub fn shards_path(&self) -> std::path::PathBuf {
        self.path.join("shards")
    }

    /// Write chunk data to this disk's shards directory.
    pub fn write(&self, chunk_id: &str, data: &[u8], cksum: u128) -> StorageResult<()> {
        if *self.is_failed.lock().unwrap() {
            return Err(StorageError::DiskFailed(format!(
                "disk {} is marked as failed",
                self.path.display()
            )));
        }

        let shards_path = self.shards_path();
        std::fs::create_dir_all(&shards_path).map_err(|e| {
            StorageError::Transient(format!("failed to create shards directory: {e}"))
        })?;

        let chunk_path = shards_path.join(chunk_id);
        // Write data + checksum (16 bytes) at the end
        let mut file_data = Vec::with_capacity(data.len() + 16);
        file_data.extend_from_slice(data);
        file_data.extend_from_slice(&cksum.to_le_bytes());

        std::fs::write(&chunk_path, &file_data).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                *self.is_failed.lock().unwrap() = true;
                StorageError::DiskFailed(format!(
                    "write to {} failed: {e}",
                    self.path.display()
                ))
            } else {
                StorageError::Transient(format!(
                    "write to {} failed: {e}",
                    self.path.display()
                ))
            }
        })
    }

    /// Read chunk data from this disk's shards directory and verify the checksum.
    pub fn read(&self, chunk_id: &str) -> StorageResult<Vec<u8>> {
        if *self.is_failed.lock().unwrap() {
            return Err(StorageError::DiskFailed(format!(
                "disk {} is marked as failed",
                self.path.display()
            )));
        }

        let chunk_path = self.shards_path().join(chunk_id);
        let file_data = std::fs::read(&chunk_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::Transient(format!(
                    "chunk {} not found on disk {}: {}",
                    chunk_id,
                    self.path.display(),
                    e
                ))
            } else {
                StorageError::Transient(format!(
                    "read from {} failed: {e}",
                    self.path.display()
                ))
            }
        })?;

        if file_data.len() < 16 {
            return Err(StorageError::Transient(format!(
                "chunk {} on {} is corrupted (too short)",
                chunk_id,
                self.path.display()
            )));
        }

        let stored_cksum =
            u128::from_le_bytes(file_data[file_data.len() - 16..].try_into().unwrap());
        let data = &file_data[..file_data.len() - 16];

        let actual_cksum = checksum::checksum(data);
        if actual_cksum != stored_cksum {
            return Err(StorageError::ChecksumMismatch {
                expected: stored_cksum,
                actual: actual_cksum,
            });
        }

        Ok(data.to_vec())
    }

    /// Delete a chunk file from this disk's shards directory.
    pub fn delete_chunk(&self, chunk_id: &str) -> StorageResult<()> {
        let chunk_path = self.shards_path().join(chunk_id);
        let _ = std::fs::remove_file(&chunk_path);
        Ok(())
    }
}

/// The chunk store manages all disks and chunk-level operations.
pub struct ChunkStore {
    pub disks: Vec<Disk>,
    pub coder: ErasureCoder,
    pub num_disks: usize,
}

impl ChunkStore {
    pub fn new(config: &Config) -> StorageResult<Self> {
        config.validate().map_err(StorageError::Transient)?;

        let num_disks = config.disk_paths.len();
        let mut disks = Vec::with_capacity(num_disks);

        // Parse UUIDs from config (may be empty → generate fresh)
        let parsed_uuids: Vec<Option<crate::cluster::ClusterId>> = config
            .disk_uuids
            .iter()
            .map(|s| {
                uuid::Uuid::parse_str(s)
                    .map(|u| Some(crate::cluster::ClusterId(u)))
                    .map_err(|e| {
                        StorageError::Transient(format!(
                            "invalid disk UUID in config for slot {}: {e}",
                            s
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Build the per-disk expected UUID list.
        // If config is empty (fresh cluster), all entries are None → generate.
        // If config has values, look them up by index.
        let expected_uuids: Vec<Option<crate::cluster::ClusterId>> = (0..num_disks)
            .map(|i| parsed_uuids.get(i).cloned().flatten())
            .collect();

        for (i, expected) in expected_uuids.iter().enumerate().take(num_disks) {
            let disk_path = std::path::PathBuf::from(&config.disk_paths[i]);

            // Scan (or generate) the cluster ID for this disk
            let cluster_id =
                crate::cluster::scan_disk_cluster_id(&disk_path, expected, i)?;

            disks.push(Disk::new(disk_path, cluster_id));
        }

        Ok(Self {
            disks,
            coder: ErasureCoder::new(config),
            num_disks,
        })
    }

    /// Encode a single chunk's data into N shards and write all shards.
    ///
    /// Returns the xxHash3-128 checksum of the chunk data (before erasure
    /// encoding), for storing in `VersionMeta.chunk_checksums`.
    pub fn write_chunk(
        &self,
        chunk_id: &str,
        data: &[u8],
        _object_cksum: u128, // passed for context; not used in erasure encoding
    ) -> StorageResult<u128> {
        let chunk_cksum = checksum::checksum(data);
        let k = self.coder.data_shards();
        let shard_size = data.len().div_ceil(k); // Round up
        // Reed-Solomon requires even shard sizes
        let shard_size = shard_size.div_ceil(2) * 2; // Round up to even
        
        // Split data into K sub-shards of equal size (pad last one if needed)
        let mut data_shards: Vec<Vec<u8>> = Vec::with_capacity(k);
        for i in 0..k {
            let start = i * shard_size;
            let end = std::cmp::min(start + shard_size, data.len());
            let mut shard = if start < data.len() {
                data[start..end].to_vec()
            } else {
                vec![0u8; shard_size]
            };
            // Pad to exact shard_size
            while shard.len() < shard_size {
                shard.push(0);
            }
            data_shards.push(shard);
        }

        // Encode K data shards into N total shards
        let shard_refs: Vec<&[u8]> = data_shards.iter().map(|s| s.as_slice()).collect();
        let all_shards = self.coder.encode(&shard_refs)?;

        // Write each shard to its home disk with per-shard checksum
        for (shard_data, disk) in all_shards.iter().zip(self.disks.iter()) {
            let shard_cksum = checksum::checksum(shard_data);
            disk.write(chunk_id, shard_data, shard_cksum)?;
        }

        Ok(chunk_cksum)
    }

    /// Read ALL N shards for a chunk, returning per-shard results.
    pub fn read_all_shards(
        &self,
        chunk_id: &str,
    ) -> Vec<Result<Option<Vec<u8>>, StorageError>> {
        let mut results = Vec::with_capacity(self.num_disks);

        for disk in self.disks.iter() {
            if *disk.is_failed.lock().unwrap() {
                results.push(Err(StorageError::DiskFailed(format!(
                    "disk {} is marked as failed",
                    disk.path.display()
                ))));
                continue;
            }
            match disk.read(chunk_id) {
                Ok(data) => {
                    results.push(Ok(Some(data)));
                }
                Err(_e) => {
                    results.push(Ok(None));
                }
            }
        }

        results
    }

    /// Recover a chunk from surviving shards.
    ///
    /// `expected_chunk_size` is the original (unpadded) byte count —
    /// used to trim reconstructed data before checksum verification.
    /// Pass 0 to skip trimming.
    pub fn recover_chunk(
        &self,
        _chunk_id: &str,
        expected_chunk_checksum: u128,
        expected_chunk_size: usize,
        shard_results: &[Result<Option<Vec<u8>>, StorageError>],
    ) -> StorageResult<ReconstructResult> {
        let mut present = vec![false; self.num_disks];
        let mut shards: Vec<Option<Vec<u8>>> = vec![None; self.num_disks];

        for (i, result) in shard_results.iter().enumerate() {
            if let Ok(Some(data)) = result {
                present[i] = true;
                shards[i] = Some(data.clone());
            }
        }

        let surviving = present
            .iter()
            .zip(shards.iter())
            .filter(|(p, s)| **p && s.is_some())
            .count();

        if surviving < self.coder.data_shards() {
            return Err(StorageError::TooManyFailures);
        }

        let shard_size = shards
            .iter()
            .filter_map(|s| s.as_ref().map(|v| v.len()))
            .next()
            .ok_or(StorageError::TooManyFailures)?;

        let mut decoder =
            reed_solomon_simd::ReedSolomonDecoder::new(
                self.coder.data_shards(),
                self.coder.parity_shards(),
                shard_size,
            )?;

        for (i, (&p, shard)) in present.iter().zip(shards.iter()).enumerate() {
            if p && shard.is_some() {
                let shard_data = shard.as_ref().unwrap();
                if i < self.coder.data_shards() {
                    decoder.add_original_shard(i, shard_data)?;
                } else {
                    // Recovery shard indices are relative to recovery_count
                    decoder.add_recovery_shard(i - self.coder.data_shards(), shard_data)?;
                }
            }
        }

        let result = decoder.decode()?;

        let mut all_shards: Vec<Vec<u8>> = Vec::with_capacity(self.num_disks);
        let restored_map: std::collections::HashMap<usize, &[u8]> =
            result.restored_original_iter().collect();

        for (i, shard) in shards.iter().enumerate().take(self.num_disks) {
            if let Some(ref data) = shard {
                all_shards.push(data.clone());
            } else if let Some(restored) = restored_map.get(&i) {
                all_shards.push(restored.to_vec());
            } else {
                return Err(StorageError::TooManyFailures);
            }
        }

        // Reconstruct data from data shards only
        let mut reconstructed_data = Vec::new();
        for shard in all_shards.iter().take(self.coder.data_shards()) {
            reconstructed_data.extend_from_slice(shard);
        }

        // Trim padding that erasure coding adds to the last data shard
        let trimmed_data = if expected_chunk_size > 0 {
            &reconstructed_data[..expected_chunk_size]
        } else {
            &reconstructed_data[..]
        };

        // Verify reconstructed data against expected chunk checksum
        let actual_cksum = checksum::checksum(trimmed_data);
        if actual_cksum != expected_chunk_checksum {
            return Err(StorageError::ChecksumMismatch {
                expected: expected_chunk_checksum,
                actual: actual_cksum,
            });
        }

        // Find shards that need correction
        let mut corrections: Vec<(usize, Vec<u8>)> = Vec::new();
        for (i, result) in shard_results.iter().enumerate() {
            match result {
                Ok(None) | Err(_) => {
                    corrections.push((i, all_shards[i].clone()));
                }
                _ => {}
            }
        }

        Ok((reconstructed_data, corrections))
    }
}
