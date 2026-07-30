/// Replicated metadata store: one Fjall database per disk with automatic repair.
use std::collections::HashMap;

use fjall::{Database, KeyspaceCreateOptions};

use crate::config::Config;
use crate::disk::{VersionMeta, VersionStatus};
use crate::errors::{StorageError, StorageResult};

/// Separator used between object_key and version in internal keys.
/// Chosen as ASCII Unit Separator (\x1f) which cannot appear in object keys.
const SEP: char = '\x1f';

/// Replicated metadata store backed by one Fjall database per disk.
///
/// On writes: commits to all healthy disks in parallel, then attempts
/// to push the new version to any failed disks (repair-on-write).
///
/// On reads: reads from any healthy disk, then attempts to repair any
/// failed disks that are stale (repair-on-read).
///
/// A disk is considered failed if a write to it previously returned an error.
/// When the disk recovers (e.g., after admin replacement), call `recover_disk()`
/// to sync it from a healthy disk.
pub struct ReplicatedMetaStore {
    /// Per-disk Fjall databases.
    dbs: HashMap<usize, fjall::Database>,
    /// Per-disk keyspace (single keyspace, keys are logically separated by prefix).
    ks: HashMap<usize, fjall::Keyspace>,
    /// Indexes of disks known to have failed (IO error).
    failed_disks: std::sync::Mutex<std::collections::HashSet<usize>>,
}

impl ReplicatedMetaStore {
    /// Open (or create) one Fjall database per disk.
    pub fn new(config: &Config) -> StorageResult<Self> {
        config.validate().map_err(StorageError::Transient)?;

        let n = config.effective_replicas();
        let mut dbs = HashMap::with_capacity(n);
        let mut ks = HashMap::with_capacity(n);

        for i in 0..n {
            let disk_db_path = format!("{}/metadata", config.disk_paths[i]);
            let db = Database::builder(&disk_db_path).open()?;
            let keyspace = db.keyspace("main", KeyspaceCreateOptions::default)?;
            dbs.insert(i, db);
            ks.insert(i, keyspace);
        }

        Ok(Self {
            dbs,
            ks,
            failed_disks: std::sync::Mutex::new(std::collections::HashSet::new()),
        })
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Get a list of healthy disk indexes (not in the failed set).
    fn healthy_indexes(&self) -> Vec<usize> {
        let failed = self.failed_disks.lock().unwrap();
        self.dbs
            .keys()
            .copied()
            .filter(|i| !failed.contains(i))
            .collect()
    }

    /// Read a value from the first available healthy disk that has it.
    fn read_from_healthy(&self, key: &[u8]) -> StorageResult<Vec<u8>> {
        for &idx in self.healthy_indexes().iter() {
            let val = self
                .ks
                .get(&idx)
                .unwrap()
                .get(key)
                .map_err(|e| StorageError::KvError(format!("read from disk {idx}: {e}")))?;

            if let Some(v) = val {
                return Ok(v.to_vec());
            }
        }

        Err(StorageError::NotFound(
            "key not found on any healthy disk".to_string(),
        ))
    }

    /// Read the latest committed version metadata for an object from any healthy disk.
    fn read_latest_committed_from_healthy(
        &self,
        object_key: &str,
    ) -> StorageResult<(u64, VersionMeta)> {
        let meta_key = format!("obj:meta:{object_key}");
        let mut best_version: u64 = 0;
        let mut best_meta: Option<VersionMeta> = None;

        let healthy = self.healthy_indexes();
        if healthy.is_empty() {
            return Err(StorageError::NoHealthyDisks);
        }

        for &idx in &healthy {
            let val = self
                .ks
                .get(&idx)
                .unwrap()
                .get(meta_key.as_bytes())
                .map_err(|e| StorageError::KvError(format!("read counter from disk {idx}: {e}")))?;

            let hinted_version = match val {
                Some(bytes) if bytes.len() >= 8 => {
                    u64::from_le_bytes(bytes[..8].try_into().unwrap())
                }
                _ => 0,
            };

            if hinted_version == 0 {
                continue;
            }

            // Verify the hinted version actually exists and is committed on this disk
            if let Ok((ver, meta)) =
                self.read_version_by_number_with_disk(object_key, hinted_version, idx)
            {
                if meta.status == VersionStatus::Committed && ver > best_version {
                    best_version = ver;
                    best_meta = Some(meta);
                }
            }

            // Also scan backwards in case hinted version was deleted
            if best_version == 0 {
                for v in (1..hinted_version).rev() {
                    if let Ok((ver, meta)) =
                        self.read_version_by_number_with_disk(object_key, v, idx)
                    {
                        if meta.status == VersionStatus::Committed && ver > best_version {
                            best_version = ver;
                            best_meta = Some(meta);
                            break; // Found the highest on this disk
                        }
                    }
                }
            }
        }

        match best_meta {
            Some(meta) => Ok((best_version, meta)),
            None => Err(StorageError::NotFound(format!(
                "no committed version found for key '{object_key}'"
            ))),
        }
    }

    /// Read a specific version from a specific disk.
    fn read_version_by_number_with_disk(
        &self,
        object_key: &str,
        version: u64,
        disk_idx: usize,
    ) -> StorageResult<(u64, VersionMeta)> {
        let ver_key = format!("ver:{object_key}{SEP}{version}");
        let value = self
            .ks
            .get(&disk_idx)
            .unwrap()
            .get(ver_key.as_bytes())
            .map_err(|e| StorageError::KvError(format!("read version on disk {disk_idx}: {e}")))?;

        let Some(value_bytes) = value else {
            return Err(StorageError::NotFound(format!(
                "version {version} metadata for key '{object_key}' not found on disk {disk_idx}"
            )));
        };

        let meta = self.deserialize_meta(&value_bytes, &ver_key)?;
        Ok((meta.version, meta))
    }

    /// Serialize a `VersionMeta` into bytes for storage. Used when updating metadata.
    pub fn serialize_meta(&self, meta: &VersionMeta) -> StorageResult<Vec<u8>> {
        // Serialize metadata map as JSON
        let meta_json = serde_json::to_string(&meta.metadata)
            .map_err(|e| StorageError::KvError(format!("serialize metadata: {e}")))?;
        let meta_json_len = meta_json.len();
        let mut buf = Vec::with_capacity(
            41 + meta.chunk_checksums.len() * 16
                + 4
                + meta_json_len
                + 1
                + 4
                + meta.compressed_sizes.len() * 4,
        );
        buf.extend_from_slice(&meta.version.to_le_bytes());
        buf.push(meta.status.to_u8());
        buf.extend_from_slice(&meta.checksum.to_le_bytes());
        buf.extend_from_slice(&(meta.data_size as u64).to_le_bytes());
        // Chunk checksums: count + per-chunk checksums
        buf.extend_from_slice(&(meta.chunk_checksums.len() as u64).to_le_bytes());
        for cksum in &meta.chunk_checksums {
            buf.extend_from_slice(&cksum.to_le_bytes());
        }
        // Metadata: length-prefixed JSON (always present in new format)
        buf.extend_from_slice(&(meta_json_len as u32).to_le_bytes());
        buf.extend_from_slice(meta_json.as_bytes());
        // New fields: compression_level + compressed_sizes (appended at end for backward compat)
        // compression_level: 0xFF = None, otherwise level + 1 (since levels are >= 1)
        let enc_level = match meta.compression_level {
            Some(l) => (l + 1) as u8,
            None => 0xFF,
        };
        buf.push(enc_level);
        // compressed_sizes: count (u32) + per-chunk sizes (u32 each)
        buf.extend_from_slice(&(meta.compressed_sizes.len() as u32).to_le_bytes());
        for &sz in &meta.compressed_sizes {
            buf.extend_from_slice(&sz.to_le_bytes());
        }
        Ok(buf)
    }

    fn deserialize_meta(&self, bytes: &[u8], _object_key: &str) -> StorageResult<VersionMeta> {
        if bytes.len() < 9 {
            return Err(StorageError::KvError("version metadata too short".into()));
        }

        let version_num = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let status = VersionStatus::from_u8(bytes[8])?;
        let checksum = if bytes.len() >= 25 {
            u128::from_le_bytes(bytes[9..25].try_into().unwrap())
        } else {
            0
        };
        let data_size = if bytes.len() >= 33 {
            u64::from_le_bytes(bytes[25..33].try_into().unwrap()) as usize
        } else {
            0
        };

        // Parse chunk checksums (present if bytes >= 41: 33 base + 8 count)
        let chunk_checksums = if bytes.len() >= 41 {
            let count = u64::from_le_bytes(bytes[33..41].try_into().unwrap()) as usize;
            let cksum_start = 41;
            let cksum_end = cksum_start + count * 16;
            if bytes.len() >= cksum_end {
                bytes[cksum_start..cksum_end]
                    .chunks(16)
                    .map(|chunk| u128::from_le_bytes(chunk.try_into().unwrap()))
                    .collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // Parse metadata: length-prefixed JSON at the end of the record.
        let meta_checksums_end = 41 + chunk_checksums.len() * 16;
        let (metadata, json_len) = if bytes.len() >= meta_checksums_end + 4 {
            let meta_len = u32::from_le_bytes(
                bytes[meta_checksums_end..meta_checksums_end + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let json_start = meta_checksums_end + 4;
            let json_end = json_start + meta_len;
            let parsed = if meta_len == 0 || (json_start <= bytes.len() && json_end <= bytes.len())
            {
                if meta_len > 0 {
                    serde_json::from_slice(&bytes[json_start..json_end]).unwrap_or_default()
                } else {
                    std::collections::HashMap::new()
                }
            } else {
                std::collections::HashMap::new()
            };
            (parsed, meta_len)
        } else {
            (std::collections::HashMap::new(), 0)
        };

        // Parse optional new fields (backward compatible: only if extra bytes present after JSON)
        let new_fields_start = meta_checksums_end + 4 + json_len;
        let (compression_level, compressed_sizes) = if new_fields_start + 5 <= bytes.len() {
            let enc_level = bytes[new_fields_start];
            let level = if enc_level == 0xFF {
                None
            } else {
                Some(enc_level as i32 - 1)
            };
            let sizes_count = u32::from_le_bytes(
                bytes[new_fields_start + 1..new_fields_start + 5]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let sizes_end = new_fields_start + 5 + sizes_count * 4;
            let sizes = if bytes.len() >= sizes_end && sizes_count > 0 {
                bytes[new_fields_start + 5..sizes_end]
                    .chunks(4)
                    .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
                    .collect()
            } else {
                Vec::new()
            };
            (level, sizes)
        } else {
            // Old format: no new fields
            (None, Vec::new())
        };

        Ok(VersionMeta {
            version: version_num,
            chunk_checksums,
            checksum,
            status,
            data_size,
            last_modified: metadata.get("last-modified").cloned().unwrap_or_default(),
            metadata,
            compression_level,
            object_format: 0, // Old format versions had no compression
            compressed_sizes,
        })
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Persist all metadata databases.
    pub fn persist(&self) -> StorageResult<()> {
        for db in self.dbs.values() {
            db.persist(fjall::PersistMode::SyncData)?;
        }
        Ok(())
    }

    /// Write a batch to all healthy disks. Then attempt repair writes to failed disks.
    ///
    /// Returns an error only if NO healthy disk accepted the batch.
    /// Partial failures are repaired (write goes to healthy disks, failed disks
    /// are marked and will be repaired on the next read or write).
    pub fn write_batch(&self, batch_ops: Vec<(String, Vec<u8>)>) -> StorageResult<()> {
        let healthy = self.healthy_indexes();
        if healthy.is_empty() {
            return Err(StorageError::NoHealthyDisks);
        }

        // Attempt write to all healthy disks in parallel
        let mut results: HashMap<usize, StorageResult<()>> = HashMap::new();
        for &idx in &healthy {
            let ks = self.ks.get(&idx).unwrap().clone();
            let db = self.dbs.get(&idx).unwrap().clone();
            let batch_ops = batch_ops.clone();

            results.insert(
                idx,
                std::thread::spawn(move || {
                    let mut batch = db.batch();
                    for (key, value) in &batch_ops {
                        batch.insert(&ks, key.as_bytes(), value.as_slice());
                    }
                    batch.commit()?;
                    Ok(())
                })
                .join()
                .unwrap_or_else(|e| {
                    Err(StorageError::Transient(format!(
                        "batch write panicked: {e:?}"
                    )))
                }),
            );
        }

        // Track which healthy disks failed
        let mut healthy_failed = std::collections::HashSet::new();
        for (idx, res) in &results {
            if res.is_err() {
                healthy_failed.insert(*idx);
            }
        }

        // Mark failed disks
        {
            let mut failed = self.failed_disks.lock().unwrap();
            for &idx in &healthy_failed {
                failed.insert(idx);
            }
        }

        // If ALL healthy disks failed, return error
        let any_success = results.values().any(|r| r.is_ok());
        if !any_success {
            return Err(StorageError::NoHealthyDisks);
        }

        // Repair: write to any disk that failed during the healthy write
        let repair_ops: Vec<(String, Vec<u8>)> = batch_ops
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for &idx in &healthy_failed {
            let ks = self.ks.get(&idx).unwrap().clone();
            let db = self.dbs.get(&idx).unwrap().clone();
            let repair_ops = repair_ops.clone();

            let _ = std::thread::spawn(move || {
                let mut batch = db.batch();
                for (key, value) in &repair_ops {
                    batch.insert(&ks, key.as_bytes(), value.as_slice());
                }
                batch.commit()
            })
            .join()
            .unwrap_or(Ok(()));
        }

        // Remove successfully repaired disks from the failed set
        {
            let mut failed = self.failed_disks.lock().unwrap();
            failed.retain(|idx| !healthy_failed.contains(idx));
        }

        Ok(())
    }

    /// Delete keys from all healthy disks in parallel.
    ///
    /// Similar to `write_batch` but removes keys instead of inserting.
    pub fn delete_keys(&self, keys: Vec<String>) -> StorageResult<()> {
        let healthy = self.healthy_indexes();
        if healthy.is_empty() {
            return Err(StorageError::NoHealthyDisks);
        }

        let mut results: HashMap<usize, StorageResult<()>> = HashMap::new();
        for &idx in &healthy {
            let ks = self.ks.get(&idx).unwrap().clone();
            let db = self.dbs.get(&idx).unwrap().clone();
            let keys = keys.clone();

            results.insert(
                idx,
                std::thread::spawn(move || {
                    let mut batch = db.batch();
                    for key in &keys {
                        batch.remove(&ks, key.as_bytes());
                    }
                    batch.commit()?;
                    Ok(())
                })
                .join()
                .unwrap_or_else(|e| {
                    Err(StorageError::Transient(format!(
                        "batch delete panicked: {e:?}"
                    )))
                }),
            );
        }

        // Track which healthy disks failed
        let mut healthy_failed = std::collections::HashSet::new();
        for (idx, res) in &results {
            if res.is_err() {
                healthy_failed.insert(*idx);
            }
        }

        // Mark failed disks
        {
            let mut failed = self.failed_disks.lock().unwrap();
            for &idx in &healthy_failed {
                failed.insert(idx);
            }
        }

        // If ALL healthy disks failed, return error
        let any_success = results.values().any(|r| r.is_ok());
        if !any_success {
            return Err(StorageError::NoHealthyDisks);
        }

        // Repair: delete from any disk that failed during the healthy delete
        for &idx in &healthy_failed {
            let ks = self.ks.get(&idx).unwrap().clone();
            let db = self.dbs.get(&idx).unwrap().clone();
            let keys = keys.clone();

            let _ = std::thread::spawn(move || {
                let mut batch = db.batch();
                for key in &keys {
                    batch.remove(&ks, key.as_bytes());
                }
                batch.commit()
            })
            .join()
            .unwrap_or(Ok(()));
        }

        // Remove successfully repaired disks from the failed set
        {
            let mut failed = self.failed_disks.lock().unwrap();
            failed.retain(|idx| !healthy_failed.contains(idx));
        }

        Ok(())
    }

    /// Read a key from any healthy disk. Repairs failed disks if needed.
    ///
    /// Reads from the first healthy disk that has the key.
    /// If a failed disk has a different (stale) value, it will be repaired
    /// by writing the correct value to it.
    pub fn read(&self, key: &[u8]) -> StorageResult<Vec<u8>> {
        // Read from first healthy disk with the key
        let value = self.read_from_healthy(key)?;

        // Repair failed disks with this value
        self.repair_failed(key, &value);

        Ok(value)
    }

    /// Read the latest committed version metadata for an object.
    pub fn read_latest_committed(&self, object_key: &str) -> StorageResult<VersionMeta> {
        let (_version, meta) = self.read_latest_committed_from_healthy(object_key)?;
        Ok(meta)
    }

    /// Read the latest committed version number.
    pub fn read_latest_version(&self, object_key: &str) -> StorageResult<u64> {
        let meta = self.read_latest_committed(object_key)?;
        Ok(meta.version)
    }

    /// Convenience: read the latest committed version metadata.
    /// Alias for read_latest_committed.
    pub fn read_version(&self, object_key: &str) -> StorageResult<VersionMeta> {
        self.read_latest_committed(object_key)
    }

    /// Read a specific version metadata from any healthy disk.
    pub fn read_version_by_number(
        &self,
        object_key: &str,
        version: u64,
    ) -> StorageResult<VersionMeta> {
        self.read_version_with_disk(object_key, version)
            .map(|(_, m)| m)
    }

    /// Set a version as pending across all healthy disks.
    pub fn set_pending(&self, object_key: &str, meta: VersionMeta) -> StorageResult<()> {
        let version = meta.version;
        let ver_key = format!("ver:{object_key}{SEP}{version}");
        let ops = vec![
            (ver_key.clone(), self.serialize_meta(&meta)?),
            (
                format!("obj:meta:{object_key}"),
                version.to_le_bytes().to_vec(),
            ),
        ];

        self.write_batch(ops)
    }

    /// Promote a version from pending to committed across all healthy disks.
    ///
    /// Returns ErrVersionConflict if the current value on any healthy disk
    /// doesn't match the expected pending state.
    ///
    /// Sets the `last_modified` timestamp to the current UTC time on promotion.
    pub fn promote_version(&self, object_key: &str, target_version: u64) -> StorageResult<()> {
        let ver_key = format!("ver:{object_key}{SEP}{target_version}");

        // Read the current value from any healthy disk to verify it's pending
        let value = self.read(ver_key.as_bytes())?;
        let meta = self.deserialize_meta(&value, &ver_key)?;

        if meta.status != VersionStatus::Pending || meta.version != target_version {
            return Err(StorageError::VersionConflict);
        }

        // Update to committed status and set last_modified to now
        let mut committed_meta = meta;
        committed_meta.status = VersionStatus::Committed;
        let now = chrono::Utc::now().to_rfc3339();
        committed_meta.last_modified = now.clone();
        committed_meta
            .metadata
            .insert("last-modified".to_string(), now);

        let ops = vec![(ver_key, self.serialize_meta(&committed_meta)?)];

        self.write_batch(ops)
    }

    /// Increment and read the latest version counter.
    pub fn incr_version_counter(&self, object_key: &str) -> StorageResult<u64> {
        let meta_key = format!("obj:meta:{object_key}");

        // Read current counter from any healthy disk (default to 0 if not found)
        let value = match self.read(meta_key.as_bytes()) {
            Ok(v) => u64::from_le_bytes(v[..8].try_into().unwrap()),
            Err(StorageError::NotFound(_)) => 0,
            Err(e) => return Err(e),
        };

        let new_version = value + 1;

        let ops = vec![(meta_key, new_version.to_le_bytes().to_vec())];

        self.write_batch(ops)?;
        Ok(new_version)
    }

    /// Delete a pending version from all healthy disks.
    pub fn delete_pending(&self, object_key: &str, version: u64) -> StorageResult<()> {
        let ver_key = format!("ver:{object_key}{SEP}{version}");

        let ops = vec![(ver_key, vec![0])]; // Mark for deletion

        self.write_batch(ops)
    }

    /// Mark the latest version as deleted.
    pub fn mark_deleted(&self, object_key: &str) -> StorageResult<()> {
        let latest_version = self.read_latest_version(object_key)?;
        let ver_key = format!("ver:{object_key}{SEP}{latest_version}");
        let value = self.read(ver_key.as_bytes())?;
        let mut meta = self.deserialize_meta(&value, &ver_key)?;
        meta.status = VersionStatus::Deleted;

        let ops = vec![(ver_key, self.serialize_meta(&meta)?)];

        self.write_batch(ops)
    }

    /// Scan version entries with a prefix.
    pub fn scan_versions(
        &self,
        prefix: &str,
        limit: usize,
    ) -> StorageResult<Vec<(String, VersionMeta)>> {
        let key_prefix = if prefix.is_empty() {
            "ver:".to_string()
        } else {
            format!("ver:{prefix}")
        };

        let mut results = Vec::new();
        let mut seen_keys = std::collections::HashSet::new();

        // Scan from first healthy disk (metadata is eventually consistent across disks)
        let healthy = self.healthy_indexes();
        if healthy.is_empty() {
            return Err(StorageError::NoHealthyDisks);
        }

        for &idx in &healthy {
            if results.len() >= limit {
                break;
            }

            let ks = self.ks.get(&idx).unwrap();
            for entry in ks.prefix(key_prefix.as_bytes()) {
                if results.len() >= limit {
                    break;
                }

                let guard = entry;
                let (key_bytes, value_bytes) = guard.into_inner()?;
                let key = String::from_utf8_lossy(&key_bytes).to_string();

                // Skip chunk list entries (identified by the SEP+"chunks" suffix)
                if key.ends_with("\u{1f}chunks") {
                    continue;
                }
                if !key.starts_with("ver:") {
                    continue;
                }
                if seen_keys.contains(&key) {
                    continue;
                }
                seen_keys.insert(key.clone());

                let meta = self.deserialize_meta(&value_bytes, &key)?;
                results.push((key, meta));
            }
        }

        Ok(results)
    }

    /// Scan all chunk metadata entries.
    pub fn scan_chunks(&self) -> StorageResult<Vec<(String, u128)>> {
        let mut results = Vec::new();
        let healthy = self.healthy_indexes();
        if healthy.is_empty() {
            return Err(StorageError::NoHealthyDisks);
        }

        let idx = healthy[0];
        for entry in self.ks.get(&idx).unwrap().prefix(b"chk:") {
            let guard = entry;
            let (key_bytes, value_bytes) = guard.into_inner()?;
            let key = String::from_utf8_lossy(&key_bytes).to_string();
            let cksum = u128::from_le_bytes(value_bytes[..16].try_into().unwrap());
            results.push((key, cksum));
        }
        Ok(results)
    }

    /// Check if a key has a pending version.
    pub fn has_pending(&self, object_key: &str) -> StorageResult<bool> {
        let key_prefix = format!("ver:{object_key}{SEP}");
        let healthy = self.healthy_indexes();
        if healthy.is_empty() {
            return Ok(false);
        }

        let idx = healthy[0];
        let ks = self.ks.get(&idx).unwrap();
        for entry in ks.prefix(key_prefix.as_bytes()) {
            let (key_bytes, value_bytes) = entry.into_inner()?;
            let key = String::from_utf8_lossy(&key_bytes).to_string();
            if key.ends_with("\u{1f}chunks") {
                continue;
            }
            if let Ok(meta) = self.deserialize_meta(&value_bytes, &key) {
                if meta.status == VersionStatus::Pending {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    // -----------------------------------------------------------------------
    // Bucket operations
    // -----------------------------------------------------------------------

    /// Check whether a bucket exists.
    pub fn bucket_exists(&self, name: &str) -> StorageResult<bool> {
        let key = format!("bkt:{name}");
        self.read(key.as_bytes())
            .map(|_| true)
            .or_else(|e| match e {
                StorageError::NotFound(_) => Ok(false),
                other => Err(other),
            })
    }

    /// Create a new bucket entry. Returns an error if the bucket already exists.
    pub fn create_bucket(&self, name: &str, created_at: &str) -> StorageResult<()> {
        if self.bucket_exists(name)? {
            return Err(StorageError::BucketAlreadyExists(name.to_string()));
        }
        let key = format!("bkt:{name}");
        let data = created_at.as_bytes().to_vec();
        self.write_batch(vec![(key, data)])
    }

    /// Read bucket metadata by name.
    pub fn read_bucket(&self, name: &str) -> StorageResult<crate::disk::BucketMeta> {
        if !self.bucket_exists(name)? {
            return Err(StorageError::NotFound(format!("bucket '{name}'")));
        }
        let key = format!("bkt:{name}");
        let value = self.read(key.as_bytes())?;
        let created_at = String::from_utf8_lossy(&value).to_string();
        Ok(crate::disk::BucketMeta {
            name: name.to_string(),
            created_at,
        })
    }

    /// Delete a bucket entry. Note: this only removes the bucket metadata marker.
    /// The bucket must be empty (no objects) before calling this.
    pub fn delete_bucket(&self, name: &str) -> StorageResult<()> {
        let key = format!("bkt:{name}");
        self.delete_keys(vec![key])
    }

    /// Scan all buckets and return their metadata.
    pub fn scan_buckets(&self) -> StorageResult<Vec<crate::disk::BucketMeta>> {
        let mut results = Vec::new();
        let healthy = self.healthy_indexes();
        if healthy.is_empty() {
            return Err(StorageError::NoHealthyDisks);
        }

        let idx = healthy[0];
        let ks = self.ks.get(&idx).unwrap();
        for entry in ks.prefix(b"bkt:") {
            let (key_bytes, value_bytes) = entry.into_inner()?;
            let key = String::from_utf8_lossy(&key_bytes).to_string();
            let name = key.strip_prefix("bkt:").unwrap_or(&key).to_string();
            let created_at = String::from_utf8_lossy(&value_bytes).to_string();
            results.push(crate::disk::BucketMeta { name, created_at });
        }

        results.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Repair operations
    // -----------------------------------------------------------------------

    /// Read a version from any healthy disk (without repair side effects).
    fn read_version_with_disk(
        &self,
        object_key: &str,
        version: u64,
    ) -> StorageResult<(u64, VersionMeta)> {
        let ver_key = format!("ver:{object_key}{SEP}{version}");
        let healthy = self.healthy_indexes();
        if healthy.is_empty() {
            return Err(StorageError::NoHealthyDisks);
        }

        for &idx in &healthy {
            match self.read_version_by_number_with_disk(object_key, version, idx) {
                Ok(result) => {
                    // Trigger repair by calling read (which repairs failed disks)
                    let _ = self.read(ver_key.as_bytes());
                    return Ok(result);
                }
                Err(StorageError::NotFound(_)) => continue,
                Err(e) => return Err(e),
            }
        }

        Err(StorageError::NotFound(format!(
            "version {version} for key '{object_key}' not found on any healthy disk"
        )))
    }

    /// Repair all failed disks for a specific key by writing the correct value.
    fn repair_failed(&self, key: &[u8], value: &[u8]) {
        let mut repaired = Vec::new();
        {
            let failed = self.failed_disks.lock().unwrap();
            for &idx in failed.iter() {
                let key_clone = key.to_vec();
                let value_clone = value.to_vec();
                let ks = self.ks.get(&idx).unwrap().clone();
                let db = self.dbs.get(&idx).unwrap().clone();

                let success = std::thread::spawn(move || {
                    let mut batch = db.batch();
                    batch.insert(&ks, &key_clone, value_clone.as_slice());
                    batch.commit().is_ok()
                })
                .join()
                .unwrap_or(false);

                if success {
                    repaired.push(idx);
                    tracing::debug!(
                        "Repaired key {} on disk {}",
                        String::from_utf8_lossy(key),
                        idx
                    );
                }
            }
        }

        // Remove successfully repaired disks from the failed set
        let mut failed = self.failed_disks.lock().unwrap();
        for idx in repaired {
            failed.remove(&idx);
        }
    }

    /// Recover a specific disk: read all data from a healthy disk and
    /// write it to the recovering disk. Call this when a failed disk is
    /// replaced or comes back online.
    ///
    /// `recovering_disk_idx` must be in `dbs` but not currently active.
    /// `source_disk_idx` must be a healthy disk to read from.
    pub fn recover_disk(
        &self,
        recovering_disk_idx: usize,
        source_disk_idx: usize,
    ) -> StorageResult<()> {
        if recovering_disk_idx == source_disk_idx {
            return Err(StorageError::Transient(
                "recovering disk cannot be the source disk".to_string(),
            ));
        }

        // Verify source is healthy
        let healthy = self.healthy_indexes();
        if !healthy.contains(&source_disk_idx) {
            return Err(StorageError::Transient(format!(
                "source disk {source_disk_idx} is not healthy"
            )));
        }

        // Get the recovering disk databases
        let rec_ks = self.ks.get(&recovering_disk_idx).ok_or_else(|| {
            StorageError::Transient(format!("disk {recovering_disk_idx} not found"))
        })?;
        let rec_db = self.dbs.get(&recovering_disk_idx).ok_or_else(|| {
            StorageError::Transient(format!("disk {recovering_disk_idx} not found"))
        })?;

        // Collect all entries from source
        let source_ks = self.ks.get(&source_disk_idx).unwrap();
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        for entry in source_ks.prefix(b"") {
            let (key, value) = entry.into_inner()?;
            entries.push((key.to_vec(), value.to_vec()));
        }

        // Write all entries to recovering disk
        for (key, value) in &entries {
            let mut batch = rec_db.batch();
            batch.insert(rec_ks, key, value.as_slice());
            batch.commit()?;
        }

        // Remove from failed set
        let mut failed = self.failed_disks.lock().unwrap();
        failed.remove(&recovering_disk_idx);

        tracing::info!(
            "Recovered disk {}, synced {} entries from disk {}",
            recovering_disk_idx,
            entries.len(),
            source_disk_idx
        );

        Ok(())
    }

    /// Get the number of healthy disks.
    pub fn healthy_disk_count(&self) -> usize {
        self.healthy_indexes().len()
    }

    /// Get the total number of disks.
    pub fn total_disk_count(&self) -> usize {
        self.dbs.len()
    }

    /// Check if system has enough healthy disks to tolerate one more failure.
    pub fn is_safe(&self) -> bool {
        self.healthy_disk_count() > self.total_disk_count() / 2
    }
}

// ---------------------------------------------------------------------------
// Chunk ID serialization helpers
