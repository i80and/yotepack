use crate::api::CHUNK_SIZE_DEFAULT;
use crate::checksum::per_chunk_checksum;
/// Disk management and chunk storage operations.
use crate::config::Config;
use crate::erasure::ErasureCoder;
use crate::errors::{StorageError, StorageResult};
use std::fs::OpenOptions;
use std::io::{Read, Seek, Write};

/// Status of a version in the KV store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VersionStatus {
    Committed = 0x00,
    Pending = 0x01,
    Deleted = 0x02,
}

/// Metadata for a bucket stored in the replicated meta store.
#[derive(Debug, Clone)]
pub struct BucketMeta {
    pub name: String,
    pub created_at: String,
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
    /// Per-chunk xxHash3-128 checksums (of chunk data before erasure encoding).
    pub chunk_checksums: Vec<u128>,
    pub checksum: u128,
    pub status: VersionStatus,
    pub data_size: usize,
    /// Time this version was committed, stored as an RFC 3339 / ISO 8601 string in UTC.
    pub last_modified: String,
    /// Arbitrary user-defined metadata (e.g., Content-Type, ACL, custom headers).
    /// Keys are lowercase; values may be any string.
    pub metadata: std::collections::HashMap<String, String>,
    /// Compression level used (None = uncompressed). Stored for round-trip consistency.
    pub compression_level: Option<i32>,
    /// Compressed size of each chunk in bytes (only valid when compression_level is Some).
    pub compressed_sizes: Vec<u32>,
}

/// Handle for an in-progress mega-file write.
pub struct WriteHandle {
    pub version: u64,
    pub chunk_size: u32,
    pub actual_data_len: usize,
    file: std::fs::File,
    tmp_path: std::path::PathBuf,
    final_path: std::path::PathBuf,
}

impl WriteHandle {
    /// Commit: sync and rename temp file to final segments/ location.
    pub fn commit_write(self) -> StorageResult<()> {
        // Sync to disk before rename
        self.file
            .sync_data()
            .map_err(|e| StorageError::Transient(format!("failed to sync mega-file: {e}")))?;

        // Atomic rename
        std::fs::rename(&self.tmp_path, &self.final_path)
            .map_err(|e| StorageError::Transient(format!("failed to commit mega-file: {e}")))?;

        // Clean up stale temp file
        let _ = std::fs::remove_file(&self.tmp_path);

        Ok(())
    }
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

    /// Returns the path to the segments directory for this disk, scoped by object key.
    pub fn segments_path(&self, object_key: &str) -> std::path::PathBuf {
        // Encode object_key as a filesystem-safe subdirectory name
        let safe_key = object_key
            .replace('/', "_")
            .replace(':', "")
            .replace('\\', "_");
        self.path.join("segments").join(safe_key)
    }

    /// Returns the path to the wip directory for this disk, scoped by object key.
    pub fn wip_path(&self, object_key: &str) -> std::path::PathBuf {
        let safe_key = object_key
            .replace('/', "_")
            .replace(':', "")
            .replace('\\', "_");
        self.path.join("wip").join(safe_key)
    }

    /// Clean up incomplete writes (wipe all wip/). Called on startup.
    pub fn cleanup_wip(&self) -> StorageResult<()> {
        let wip_parent = self.path.join("wip");
        if wip_parent.exists() {
            for entry in std::fs::read_dir(&wip_parent)
                .map_err(|e| StorageError::Transient(format!("failed to read wip dir: {e}")))?
            {
                let entry = entry.map_err(|e| {
                    StorageError::Transient(format!("failed to read wip entry: {e}"))
                })?;
                let wip_dir = entry.path();
                if wip_dir.is_dir() {
                    // Remove all temp files in this object-key subdirectory
                    for tmp_entry in std::fs::read_dir(&wip_dir).map_err(|e| {
                        StorageError::Transient(format!("failed to read wip subdir: {e}"))
                    })? {
                        let tmp_entry = tmp_entry.map_err(|e| {
                            StorageError::Transient(format!("failed to read tmp entry: {e}"))
                        })?;
                        let _ = std::fs::remove_file(tmp_entry.path());
                    }
                } else {
                    // Legacy flat wip file
                    let _ = std::fs::remove_file(wip_dir);
                }
            }
        }
        Ok(())
    }

    /// Open a mega-file for streaming append. Returns a write handle.
    /// Temp file is created in wip/, final destination is segments/.
    /// `object_format` byte at offset 0: 0=uncompressed, 1=zstd compressed.
    pub fn open_write(
        &self,
        object_key: &str,
        version: u64,
        chunk_size: u32,
        object_format: u8,
    ) -> StorageResult<WriteHandle> {
        let wip = self.wip_path(object_key);
        std::fs::create_dir_all(&wip)
            .map_err(|e| StorageError::Transient(format!("failed to create wip dir: {e}")))?;

        // Also ensure segments directory exists
        let segments = self.segments_path(object_key);
        std::fs::create_dir_all(&segments)
            .map_err(|e| StorageError::Transient(format!("failed to create segments dir: {e}")))?;

        let uuid = uuid::Uuid::new_v4();
        let tmp_path = wip.join(format!("{uuid}.tmp"));

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| StorageError::Transient(format!("failed to create mega-file: {e}")))?;

        // Write object_format as header (1 byte + 3 bytes padding to maintain alignment)
        file.write_all(&[object_format, 0, 0, 0])
            .map_err(|e| StorageError::Transient(format!("failed to write header: {e}")))?;

        let final_path = segments.join(format!("v{version:08}"));

        Ok(WriteHandle {
            version,
            chunk_size,
            actual_data_len: 0,
            file,
            tmp_path,
            final_path,
        })
    }

    /// Append one chunk's shard segment to the mega-file.
    /// Format: <cksum:u128><len:u64><data>
    pub fn append_chunk(
        &self,
        handle: &mut WriteHandle,
        data: &[u8],
        cksum: u128,
    ) -> StorageResult<()> {
        handle
            .file
            .write_all(&cksum.to_le_bytes())
            .map_err(|e| StorageError::Transient(format!("failed to write checksum: {e}")))?;
        handle
            .file
            .write_all(&(data.len() as u64).to_le_bytes())
            .map_err(|e| StorageError::Transient(format!("failed to write length: {e}")))?;
        handle.file.write_all(data).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                *self.is_failed.lock().unwrap() = true;
                StorageError::DiskFailed(format!("write to {} failed: {e}", self.path.display()))
            } else {
                StorageError::Transient(format!("write to {} failed: {e}", self.path.display()))
            }
        })?;
        Ok(())
    }

    /// Read all chunk entries from a mega-file sequentially.
    /// Read a single chunk entry from a mega-file.
    /// If `target_chunk` is provided, seeks directly to that chunk's entry.
    /// `object_format` is the format byte (0=uncompressed, 1=compressed).
    /// `compressed_shard_size` is the shard data size after erasure coding.
    /// Returns Vec with one result (the target shard).
    pub fn read_all_shards(
        &self,
        object_key: &str,
        target_chunk: Option<usize>,
        object_format: u8,
        compressed_shard_size: usize,
        version: u64,
    ) -> StorageResult<Vec<Result<Vec<u8>, StorageError>>> {
        if *self.is_failed.lock().unwrap() {
            return Err(StorageError::DiskFailed(format!(
                "disk {} is marked as failed",
                self.path.display()
            )));
        }

        let path = self
            .segments_path(object_key)
            .join(format!("v{version:08}"));
        let file = std::fs::File::open(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::Transient(format!(
                    "version {} not found on disk {}: {}",
                    version,
                    self.path.display(),
                    e
                ))
            } else {
                StorageError::Transient(format!("read from {} failed: {e}", self.path.display()))
            }
        })?;
        let mut reader = std::io::BufReader::new(file);

        // Read header (4 bytes: object_format + 3 bytes padding)
        let mut _header = [0u8; 4];
        reader
            .read_exact(&mut _header)
            .map_err(|e| StorageError::Transient(format!("failed to read header: {e}")))?;

        let is_compressed = object_format == 1;
        let mut results = Vec::new();

        if let Some(chunk_idx) = target_chunk {
            // Direct seek to target chunk's entry
            // Entry format: <cksum:u128><len:u64><data> = 24 bytes header + data
            let offset = 4 + chunk_idx * (24 + compressed_shard_size);
            reader
                .seek(std::io::SeekFrom::Start(offset as u64))
                .map_err(|e| StorageError::Transient(format!("seek failed: {e}")))?;

            let mut cksum_buf = [0u8; 16];
            reader
                .read_exact(&mut cksum_buf)
                .map_err(|e| StorageError::Transient(format!("read checksum failed: {e}")))?;
            let stored_cksum = u128::from_le_bytes(cksum_buf);

            let mut len_buf = [0u8; 8];
            reader
                .read_exact(&mut len_buf)
                .map_err(|e| StorageError::Transient(format!("read length failed: {e}")))?;
            let data_len = u64::from_le_bytes(len_buf) as usize;

            let mut data = vec![0u8; data_len];
            reader
                .read_exact(&mut data)
                .map_err(|e| StorageError::Transient(format!("read data failed: {e}")))?;

            // Verify checksum to detect bitrot
            let actual_cksum = per_chunk_checksum(&data);
            if actual_cksum != stored_cksum {
                tracing::warn!(
                    "Checksum mismatch on disk {}: stored={:#034x} actual={:#034x} — shard will be repaired on next write",
                    self.path.display(), stored_cksum, actual_cksum
                );
                results.push(Err(StorageError::ChecksumMismatch {
                    expected: stored_cksum,
                    actual: actual_cksum,
                }));
            } else {
                // Decompress if needed
                let data = if is_compressed {
                    zstd::decode_all(data.as_slice())
                        .map_err(|e| StorageError::Transient(format!("zstd decode shard: {e}")))?
                } else {
                    data
                };

                results.push(Ok(data));
            }
        } else {
            // Sequential read for full enumeration
            loop {
                // Read checksum (16 bytes)
                let mut cksum_buf = [0u8; 16];
                match reader.read_exact(&mut cksum_buf) {
                    Ok(()) => {}
                    Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => {
                        return Err(StorageError::Transient(format!(
                            "read checksum failed: {e}"
                        )));
                    }
                }

                let mut len_buf = [0u8; 8];
                match reader.read_exact(&mut len_buf) {
                    Ok(()) => {}
                    Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => {
                        return Err(StorageError::Transient(format!("read length failed: {e}")));
                    }
                }
                let data_len = u64::from_le_bytes(len_buf) as usize;

                let mut data = vec![0u8; data_len];
                reader
                    .read_exact(&mut data)
                    .map_err(|e| StorageError::Transient(format!("read data failed: {e}")))?;

                // Decompress if needed
                let data = if is_compressed {
                    zstd::decode_all(data.as_slice())
                        .map_err(|e| StorageError::Transient(format!("zstd decode shard: {e}")))?
                } else {
                    data
                };

                results.push(Ok(data));
            }
        }

        Ok(results)
    }

    /// Fix a corrupted entry in a mega-file by rewriting it.
    /// Reads all entries, replaces the one at `chunk_idx` with `corrected_data`,
    /// then rewrites the entire file. This repairs bitrot on-disk.
    pub fn fix_mega_file_entry(
        &self,
        object_key: &str,
        chunk_idx: usize,
        corrected_data: &[u8],
        version: u64,
    ) -> StorageResult<()> {
        if *self.is_failed.lock().unwrap() {
            return Err(StorageError::DiskFailed(format!(
                "disk {} is marked as failed",
                self.path.display()
            )));
        }

        let path = self
            .segments_path(object_key)
            .join(format!("v{version:08}"));

        // Read all existing entries
        let raw = std::fs::read(&path)
            .map_err(|e| StorageError::Transient(format!("read mega-file failed: {e}")))?;

        let mut out = Vec::new();
        out.extend_from_slice(&raw[..4]); // keep header

        let mut idx = 4;
        let mut written = 0u64;
        while idx < raw.len() {
            let remaining = raw.len() - idx;
            if remaining < 24 {
                break; // incomplete entry
            }
            let len = u64::from_le_bytes(raw[idx + 16..idx + 24].try_into().unwrap()) as usize;
            if written as usize == chunk_idx {
                // Replace with corrected entry
                let cksum = per_chunk_checksum(corrected_data);
                out.extend_from_slice(&cksum.to_le_bytes());
                out.extend_from_slice(&(corrected_data.len() as u64).to_le_bytes());
                out.extend_from_slice(corrected_data);
                tracing::info!(
                    "Bitrot repaired: chunk {chunk_idx} on disk {} → {} bytes",
                    self.path.display(),
                    corrected_data.len()
                );
            } else {
                // Keep original entry
                out.extend_from_slice(&raw[idx..idx + 24 + len]);
            }
            idx += 24 + len;
            written += 1;
        }

        // Write atomically via tmp + rename
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &out)
            .map_err(|e| StorageError::Transient(format!("write correction failed: {e}")))?;
        std::fs::rename(&tmp, &path)
            .map_err(|e| StorageError::Transient(format!("rename correction failed: {e}")))?;

        Ok(())
    }
}

/// The chunk store manages all disks and chunk-level operations.
pub struct ChunkStore {
    pub disks: Vec<Disk>,
    pub coder: ErasureCoder,
    pub num_disks: usize,
    pub k: usize, // Number of data shards (fixed per cluster)
    /// Zstd compression level. None = no compression.
    pub compression_level: Option<i32>,
}

impl ChunkStore {
    pub fn new(config: &Config) -> StorageResult<Self> {
        config.validate().map_err(StorageError::Transient)?;

        let num_disks = config.disk_paths.len();
        let coder = ErasureCoder::new(config);
        let k = coder.data_shards();
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
            let cluster_id = crate::cluster::scan_disk_cluster_id(&disk_path, expected, i)?;

            disks.push(Disk::new(disk_path, cluster_id));
        }

        Ok(Self {
            disks,
            coder,
            num_disks,
            k,
            compression_level: config.compression_level,
        })
    }

    /// Encode a single chunk's data into N shards and append to mega-files.
    /// The caller must have opened mega-files (via Disk::open_write) before
    /// calling this. Each disk's handle is stored in `handles[disk_idx]`.
    ///
    /// If compression is enabled, compresses before erasure coding.
    /// Returns: (per-chunk checksum, compressed shard size in bytes)
    pub fn encode_and_append(
        &self,
        chunk_data: &[u8],
        _chunk_size: u32,
        is_last: bool,
        handles: &mut [Option<crate::disk::WriteHandle>],
    ) -> StorageResult<(u128, u64)> {
        let chunk_cksum = per_chunk_checksum(chunk_data);

        if let Some(level) = self.compression_level {
            // Compressed path: compress, then erasure-code the compressed data
            let mut compressed = zstd::encode_all(chunk_data, level)
                .map_err(|e| StorageError::Transient(format!("zstd encode: {e}")))?;
            let k = self.k;

            let (padded_len, shard_size) = if is_last {
                let s = compressed.len().div_ceil(k);
                let s = s.div_ceil(2) * 2;
                (s * k, s)
            } else {
                (CHUNK_SIZE_DEFAULT, CHUNK_SIZE_DEFAULT.div_ceil(k))
            };

            compressed.extend_from_slice(&vec![0u8; padded_len - compressed.len()]);

            let mut data_shards: Vec<Vec<u8>> = Vec::with_capacity(k);
            for i in 0..k {
                let start = i * shard_size;
                let end = start + shard_size;
                data_shards.push(compressed[start..end].to_vec());
            }

            let shard_refs: Vec<&[u8]> = data_shards.iter().map(|s| s.as_slice()).collect();
            let all_shards = self.coder.encode(&shard_refs)?;

            for (disk_idx, (shard_data, _disk)) in
                all_shards.iter().zip(self.disks.iter()).enumerate()
            {
                let shard_cksum = per_chunk_checksum(shard_data);
                if let Some(ref mut handle) = handles[disk_idx] {
                    _disk.append_chunk(handle, shard_data, shard_cksum)?;
                }
            }

            // Store total encoded size (24-byte entry header + shard_size) * num_disks
            let total_encoded_size = (24 + shard_size) * self.num_disks;
            return Ok((chunk_cksum, total_encoded_size as u64));
        }

        // Uncompressed path
        let k = self.k;
        let (padded_len, shard_size) = if is_last {
            let s = chunk_data.len().div_ceil(k);
            let s = s.div_ceil(2) * 2;
            (s * k, s)
        } else {
            (CHUNK_SIZE_DEFAULT, CHUNK_SIZE_DEFAULT.div_ceil(k))
        };

        // Pad chunk_data to padded_len
        let mut padded = vec![0u8; padded_len];
        padded[..chunk_data.len()].copy_from_slice(chunk_data);

        // Split into K sub-shards (each exactly shard_size)
        let mut data_shards: Vec<Vec<u8>> = Vec::with_capacity(k);
        for i in 0..k {
            let start = i * shard_size;
            let end = start + shard_size;
            data_shards.push(padded[start..end].to_vec());
        }

        // Encode K data shards into N total shards
        let shard_refs: Vec<&[u8]> = data_shards.iter().map(|s| s.as_slice()).collect();
        let all_shards = self.coder.encode(&shard_refs)?;

        // Append each shard segment to its disk's mega-file
        for (disk_idx, (shard_data, _disk)) in all_shards.iter().zip(self.disks.iter()).enumerate()
        {
            let shard_cksum = per_chunk_checksum(shard_data);
            if let Some(ref mut handle) = handles[disk_idx] {
                _disk.append_chunk(handle, shard_data, shard_cksum)?;
            }
        }

        // For metadata, the "compressed size" is total encoded size across all shards
        // (24-byte entry header + shard_size) * num_disks
        let total_encoded_size = (24 + shard_size) * self.num_disks;
        Ok((chunk_cksum, total_encoded_size as u64))
    }

    /// Read a single chunk's shard from the mega-file for a given version.
    /// Seeks directly to the chunk's entry (one seek per disk).
    /// Returns per-disk results for recovery.
    /// `object_format` tells whether the object is compressed.
    /// `compressed_sizes` gives the compressed size of each chunk (for entry_size calculation).
    pub fn read_chunk(
        &self,
        object_key: &str,
        chunk_idx: usize,
        object_format: u8,
        compressed_sizes: &[u32],
        version: u64,
    ) -> Vec<Result<Vec<u8>, StorageError>> {
        // Entry format: <cksum:u128><len:u64><data> = 24 bytes header + data
        // total_per_disk = compressed_sizes[chunk_idx] / num_disks = 24 + shard_size
        let total_per_disk = compressed_sizes[chunk_idx] as usize / self.num_disks;
        let compressed_shard_size = total_per_disk - 24;
        let _entry_size = 24 + compressed_shard_size;

        let mut results = Vec::with_capacity(self.num_disks);

        for disk in self.disks.iter() {
            if *disk.is_failed.lock().unwrap() {
                results.push(Err(StorageError::DiskFailed(format!(
                    "disk {} is marked as failed",
                    disk.path.display()
                ))));
                continue;
            }
            match disk.read_all_shards(
                object_key,
                Some(chunk_idx),
                object_format,
                compressed_shard_size,
                version,
            ) {
                Ok(all_shards) => {
                    if all_shards.is_empty() {
                        results.push(Err(StorageError::Transient(format!(
                            "no shards returned for chunk {} version {} on disk {}",
                            chunk_idx,
                            version,
                            disk.path.display()
                        ))));
                    } else {
                        match &all_shards[0] {
                            Ok(data) => results.push(Ok(data.clone())),
                            Err(e) => results.push(Err(e.clone())),
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        "Failed to read version {} on disk {}: {}",
                        version,
                        disk.path.display(),
                        e
                    );
                    results.push(Err(e));
                }
            }
        }

        results
    }

    /// Recover a chunk from surviving shards.
    ///
    /// `expected_chunk_size` is the original (unpadded) byte count —
    /// used to trim reconstructed data before checksum verification.
    pub fn recover_chunk(
        &self,
        expected_chunk_checksum: u128,
        expected_chunk_size: usize,
        shard_results: &[Result<Vec<u8>, StorageError>],
    ) -> StorageResult<ReconstructResult> {
        let mut present = vec![false; self.num_disks];
        let mut shards: Vec<Option<Vec<u8>>> = vec![None; self.num_disks];

        for (i, result) in shard_results.iter().enumerate() {
            if let Ok(ref data) = result {
                present[i] = true;
                shards[i] = Some(data.clone());
                tracing::debug!("recover_chunk: disk {} has {} bytes", i, data.len());
            }
        }

        let surviving = present
            .iter()
            .zip(shards.iter())
            .filter(|(p, s)| **p && s.is_some())
            .count();

        if surviving < self.coder.data_shards() {
            tracing::error!(
                "recover_chunk: only {} of {} shards present",
                surviving,
                self.num_disks
            );
            for (i, r) in shard_results.iter().enumerate() {
                match r {
                    Ok(d) => tracing::error!("  disk {}: {} bytes", i, d.len()),
                    Err(e) => tracing::error!("  disk {}: {:?}", i, e),
                }
            }
            return Err(StorageError::TooManyFailures);
        }

        let shard_size = shards
            .iter()
            .filter_map(|s| s.as_ref().map(|v| v.len()))
            .next()
            .ok_or(StorageError::TooManyFailures)?;

        let mut decoder = reed_solomon_simd::ReedSolomonDecoder::new(
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
        let actual_cksum = per_chunk_checksum(trimmed_data);
        if actual_cksum != expected_chunk_checksum {
            return Err(StorageError::ChecksumMismatch {
                expected: expected_chunk_checksum,
                actual: actual_cksum,
            });
        }

        // Find shards that need correction (bitrot recovery)
        let mut corrections: Vec<(usize, Vec<u8>)> = Vec::new();
        for (i, result) in shard_results.iter().enumerate() {
            if result.is_err() {
                corrections.push((i, all_shards[i].clone()));
            }
        }

        Ok((reconstructed_data, corrections))
    }
}
