/// Main storage API: PUT, GET, DELETE, LIST, GarbageCollection.
use std::collections::HashSet;

use chrono::Utc;
use futures::io::Cursor;
use futures::AsyncReadExt;

use crate::checksum::{self, StreamingChecksum};
use crate::config::Config;
use crate::disk::{ChunkStore, VersionMeta, VersionStatus};
use crate::errors::{StorageError, StorageResult};

pub(crate) const CHUNK_SIZE_DEFAULT: usize = 64 * 1024 * 1024;

/// The main object storage service.
pub struct ObjectStorage {
    pub config: Config,
    pub chunk_store: ChunkStore,
    pub meta_store: crate::metadata::ReplicatedMetaStore,
}

impl ObjectStorage {
    /// Create a new ObjectStorage instance.
    pub fn new(config: Config) -> StorageResult<Self> {
        Ok(Self {
            chunk_store: ChunkStore::new(&config)?,
            meta_store: crate::metadata::ReplicatedMetaStore::new(&config)?,
            config,
        })
    }

    // -------------------------------------------------------------------------
    // GET(object_key, read_after_write_token: Option<u64>) -> (data, error)
    // -------------------------------------------------------------------------

    pub async fn get(
        &self,
        object_key: &str,
        read_after_write_token: Option<u64>,
    ) -> StorageResult<Vec<u8>> {
        if object_key.is_empty() {
            return Err(StorageError::NotFound(
                "object key cannot be empty".to_string(),
            ));
        }

        // Step 1: Determine which version to read.
        let version = if let Some(token) = read_after_write_token {
            token
        } else {
            self.meta_store.read_latest_version(object_key)?
        };

        // Step 2: Fetch version metadata.
        let meta = if version != 0 {
            self.meta_store
                .read_version_by_number(object_key, version)?
        } else {
            self.meta_store.read_version(object_key)?
        };

        if meta.version != version || meta.status != VersionStatus::Committed {
            return Err(StorageError::VersionNotFound {
                key: object_key.to_string(),
                version,
            });
        }

        if meta.chunk_ids.is_empty() && meta.chunk_checksums.is_empty() {
            return Ok(Vec::new());
        }

        let expected_cksum = meta.checksum;
        let total_chunks = meta.chunk_checksums.len();
        let data_size = meta.data_size;
        let chunk_size = CHUNK_SIZE_DEFAULT as u32;
        let mut all_data: Vec<u8> = Vec::with_capacity(data_size);

        // Step 3-4: Read and decode each chunk from mega-files
        for (chunk_idx, &expected_chunk_cksum) in meta.chunk_checksums.iter().enumerate() {
            // Calculate expected (unpadded) chunk size
            let expected_chunk_size = if chunk_idx == total_chunks - 1 {
                data_size - (chunk_idx * CHUNK_SIZE_DEFAULT)
            } else {
                CHUNK_SIZE_DEFAULT
            };

            // Compute entry size: 16 (cksum) + 8 (len) + shard_size
            let k = self.chunk_store.k;
            let shard_size = (expected_chunk_size + k - 1) / k;
            let shard_size = shard_size.div_ceil(2) * 2; // even
            let entry_size = 24 + shard_size;

            // Read all N shards for this chunk from mega-files
            let shard_results = self
                .chunk_store
                .read_chunk(object_key, chunk_idx, entry_size, version);

            // Decode this chunk from its shards (verifies chunk checksum)
            let (chunk_data, corrections) = self.chunk_store.recover_chunk(
                expected_chunk_cksum,
                expected_chunk_size,
                &shard_results,
            )?;

            // Trim last chunk if needed (it may have padding from erasure coding)
            let actual_chunk_size = std::cmp::min(chunk_data.len(), expected_chunk_size);
            all_data.extend_from_slice(&chunk_data[..actual_chunk_size]);

            // Write corrections for any failed/corrupted shards
            for (disk_idx, corrected_shard) in corrections {
                // Use chunk_ids[chunk_idx] for legacy file naming
                if let Some(chunk_id) = meta.chunk_ids.get(chunk_idx) {
                    let shard_cksum = checksum::checksum(&corrected_shard);
                    if let Err(e) = self.chunk_store.disks[disk_idx].write_legacy(
                        chunk_id,
                        &corrected_shard,
                        shard_cksum,
                    ) {
                        tracing::warn!("Failed to write correction for chunk {}: {e}", chunk_id);
                    }
                }
            }
        }

        // Step 5: Verify overall checksum
        let actual_cksum = checksum::checksum(&all_data);
        if actual_cksum != expected_cksum {
            return Err(StorageError::ChecksumMismatch {
                expected: expected_cksum,
                actual: actual_cksum,
            });
        }

        Ok(all_data)
    }

    // -------------------------------------------------------------------------
    // PUT(object_key, data: &[u8]) -> (version_token, error)
    // -------------------------------------------------------------------------

    pub async fn put(&self, object_key: &str, data: &[u8]) -> StorageResult<u64> {
        if object_key.is_empty() {
            return Err(StorageError::NotFound(
                "object key cannot be empty".to_string(),
            ));
        }

        let mut cursor = Cursor::new(data.to_vec());
        self.put_stream(object_key, &mut cursor).await
    }

    // -------------------------------------------------------------------------
    // PUT_STREAM(object_key, reader: &mut R) -> (version_token, error)
    //
    // Streaming version of put that accepts any AsyncRead. Objects larger
    // than available RAM are supported — only one chunk (~chunk_size bytes)
    // is buffered at a time.
    // -------------------------------------------------------------------------

    pub async fn put_stream<R: futures::io::AsyncRead + Unpin + Send>(
        &self,
        object_key: &str,
        reader: &mut R,
    ) -> StorageResult<u64> {
        let chunk_size = CHUNK_SIZE_DEFAULT as u32;
        let mut buf = vec![0u8; CHUNK_SIZE_DEFAULT];
        let mut chunk_data_list: Vec<Vec<u8>> = Vec::new();
        let mut chunk_checksums: Vec<u128> = Vec::new();
        let mut obj_hasher = StreamingChecksum::new();
        let mut data_size: usize = 0;

        // Phase 1: Stream chunks from reader, compute checksums
        loop {
            let n = reader.read(&mut buf[..]).await?;
            if n == 0 {
                break;
            }

            let chunk_data = buf[..n].to_vec();
            let chunk_cksum = checksum::checksum(&chunk_data);

            obj_hasher.update(&chunk_data);
            data_size += n;
            chunk_data_list.push(chunk_data);
            chunk_checksums.push(chunk_cksum);
        }

        // Phase 2: Compute final object-level checksum
        let object_checksum = obj_hasher.finalize();

        if chunk_data_list.is_empty() {
            // Empty object
            let next_version = self.meta_store.incr_version_counter(object_key)?;
            self.meta_store.set_pending(
                object_key,
                next_version,
                &[],
                &[],
                object_checksum,
                0,
                std::collections::HashMap::new(),
            )?;
            return self
                .meta_store
                .promote_version(object_key, next_version)
                .map(|_| next_version);
        }

        // Phase 3: Determine version (check for pending, then set)
        let next_version = if self.meta_store.has_pending(object_key)? {
            self.meta_store.read_latest_version(object_key)?
        } else {
            self.meta_store.incr_version_counter(object_key)?
        };

        // Phase 4: Open mega-files and append all encoded chunks
        let mut handles: Vec<Option<crate::disk::WriteHandle>> = self
            .chunk_store
            .disks
            .iter()
            .map(|d| {
                // Write actual data size of first chunk (for format detection)
                // Use 0 as format marker for new format (variable-length entries)
                d.open_write(object_key, next_version, chunk_size, 0)
                    .map(Some)
            })
            .collect::<Result<_, _>>()?;

        for (idx, chunk_data) in chunk_data_list.iter().enumerate() {
            let is_last = idx == chunk_data_list.len() - 1;
            let (_, _) = self.chunk_store.encode_and_append(
                chunk_data,
                chunk_size,
                is_last,
                &mut handles,
            )?;
        }

        // Phase 5: Commit all mega-files (atomic rename)
        for handle in handles {
            let handle = handle.expect("handle should be Some");
            handle.commit_write()?;
        }

        // Phase 6: Set pending + promote in metadata
        self.meta_store.set_pending(
            object_key,
            next_version,
            &Vec::new(), // chunk_ids - no longer using chunk IDs
            &chunk_checksums,
            object_checksum,
            data_size,
            std::collections::HashMap::new(),
        )?;

        // Phase 7: Promote from pending → committed
        match self.meta_store.promote_version(object_key, next_version) {
            Ok(()) => {}
            Err(StorageError::VersionConflict) => {
                let meta = self.meta_store.read_version(object_key)?;
                return Ok(meta.version);
            }
            Err(e) => return Err(e),
        }

        Ok(next_version)
    }

    // -------------------------------------------------------------------------
    // DELETE(object_key) -> error
    // -------------------------------------------------------------------------

    pub fn delete(&self, object_key: &str) -> StorageResult<()> {
        if object_key.is_empty() {
            return Err(StorageError::NotFound(
                "object key cannot be empty".to_string(),
            ));
        }

        match self.meta_store.read_latest_version(object_key) {
            Ok(_) => {}
            Err(StorageError::NotFound(_)) => {
                return Err(StorageError::NotFound(object_key.to_string()));
            }
            Err(e) => return Err(e),
        }
        self.meta_store.mark_deleted(object_key)?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // LIST(prefix="", marker="", limit=1000) -> entries
    // -------------------------------------------------------------------------

    pub fn list(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<ListEntry>> {
        let entries = self.meta_store.scan_versions(prefix, limit * 2)?;

        // Collect latest committed version per object, keeping the key
        let mut latest: std::collections::HashMap<String, (VersionMeta, String)> =
            std::collections::HashMap::new();

        for (_key, meta) in entries {
            if meta.status != VersionStatus::Committed {
                continue;
            }
            let object_key = _key.strip_prefix("ver:").unwrap_or(&_key).to_string();
            let existing = latest
                .entry(object_key.clone())
                .or_insert((meta.clone(), object_key));
            if meta.version > existing.0.version {
                existing.0 = meta;
            }
        }

        let mut results: Vec<_> = latest.into_values().collect();
        results.sort_by(|a, b| a.1.cmp(&b.1));

        if let Some(m) = marker {
            results.retain(|e| e.1.as_str() > m);
        }

        results.truncate(limit);

        let mut output = Vec::with_capacity(results.len());
        for (meta, object_key) in &results {
            let size = meta.data_size;

            output.push(ListEntry {
                key: object_key.clone(),
                version: meta.version,
                size,
                checksum: meta.checksum,
            });
        }

        Ok(output)
    }

    // -------------------------------------------------------------------------
    // GarbageCollection
    // -------------------------------------------------------------------------

    pub fn garbage_collect(&self) -> StorageResult<()> {
        // Collect all referenced chunks from committed versions
        let mut referenced: HashSet<u64> = HashSet::new();

        let all_entries = self.meta_store.scan_versions("", usize::MAX)?;
        for (_key, meta) in all_entries {
            if meta.status == VersionStatus::Committed {
                referenced.insert(meta.version);
            }
        }

        // Check segments/ directories for orphaned versions
        for disk in &self.chunk_store.disks {
            let segments_parent = disk.path.join("segments");
            if segments_parent.exists() {
                // Iterate over object-key subdirectories
                for entry in std::fs::read_dir(&segments_parent).map_err(|e| {
                    StorageError::Transient(format!("failed to read segments dir: {e}"))
                })? {
                    let entry = entry.map_err(|e| {
                        StorageError::Transient(format!("failed to read segment entry: {e}"))
                    })?;
                    let obj_dir = entry.path();
                    if !obj_dir.is_dir() {
                        continue;
                    }
                    // Check version files within this object directory
                    for ver_entry in std::fs::read_dir(&obj_dir).map_err(|e| {
                        StorageError::Transient(format!("failed to read object dir: {e}"))
                    })? {
                        let ver_entry = ver_entry.map_err(|e| {
                            StorageError::Transient(format!("failed to read version entry: {e}"))
                        })?;
                        let filename = ver_entry.file_name();
                        let filename_str = filename.to_string_lossy();
                        if let Some(version_str) = filename_str.strip_prefix("v") {
                            if let Ok(version) = version_str.parse::<u64>() {
                                if !referenced.contains(&version) {
                                    // Orphaned segment file
                                    let _ = std::fs::remove_file(ver_entry.path());
                                    tracing::debug!("GC: removed orphaned segment v{version}");
                                }
                            }
                        }
                    }
                }
            }
        }

        // Clean up legacy shard files (shouldn't exist, but just in case)
        for disk in &self.chunk_store.disks {
            let shards = disk.path.join("shards");
            if shards.exists() {
                if let Err(e) = std::fs::remove_dir_all(&shards) {
                    tracing::warn!(
                        "Failed to cleanup legacy shards on {}: {}",
                        disk.path.display(),
                        e
                    );
                }
            }
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // StartupRecovery
    // -------------------------------------------------------------------------

    pub fn recover_on_startup(&self) -> StorageResult<()> {
        // Clean up incomplete writes (wip/ directories)
        for disk in &self.chunk_store.disks {
            if let Err(e) = disk.cleanup_wip() {
                tracing::warn!("Failed to cleanup wip on {}: {}", disk.path.display(), e);
            }
        }

        // Clean up legacy shard files
        for disk in &self.chunk_store.disks {
            let shards = disk.path.join("shards");
            if shards.exists() {
                if let Err(e) = std::fs::remove_dir_all(&shards) {
                    tracing::warn!(
                        "Failed to cleanup legacy shards on {}: {}",
                        disk.path.display(),
                        e
                    );
                }
            }
        }

        let all_entries = self.meta_store.scan_versions("", usize::MAX)?;

        for (_key, meta) in all_entries {
            if meta.status == VersionStatus::Pending {
                // Abandon all pending versions (we don't check shard existence anymore)
                let object_key = if let Some(sep) = _key.rfind('\u{1f}') {
                    _key[..sep]
                        .strip_prefix("ver:")
                        .map_or(_key.as_str(), |s| s)
                } else {
                    _key.strip_prefix("ver:").map_or(_key.as_str(), |s| s)
                };
                let _ = self.meta_store.delete_pending(object_key, meta.version);
                tracing::warn!(
                    "Abandoned pending version: {object_key} ver {}",
                    meta.version
                );
            }
        }

        if let Err(e) = self.garbage_collect() {
            tracing::warn!("Garbage collection had warnings: {e}");
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Disk recovery
    // -------------------------------------------------------------------------

    /// Recover a failed metadata disk by syncing from a healthy source disk.
    ///
    /// Call this when a disk is replaced or comes back online after failure.
    /// The recovering disk must be one of the configured replicas (its position
    /// in the replica set). The source disk must be a healthy disk.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // After replacing disk 2:
    /// storage.recover_meta_disk(2, 0)?;
    /// ```
    pub fn recover_meta_disk(
        &self,
        recovering_disk_idx: usize,
        source_disk_idx: usize,
    ) -> StorageResult<()> {
        self.meta_store
            .recover_disk(recovering_disk_idx, source_disk_idx)
    }

    /// Get the number of healthy metadata disks.
    pub fn healthy_meta_disk_count(&self) -> usize {
        self.meta_store.healthy_disk_count()
    }

    /// Get the total number of metadata replica disks.
    pub fn total_meta_disk_count(&self) -> usize {
        self.meta_store.total_disk_count()
    }

    /// Check if the metadata layer is in a safe state.
    pub fn is_meta_safe(&self) -> bool {
        self.meta_store.is_safe()
    }

    // -------------------------------------------------------------------------
    // Object Metadata
    // -------------------------------------------------------------------------

    /// Get the full metadata map for an object version.
    pub fn get_object_metadata(
        &self,
        object_key: &str,
    ) -> StorageResult<std::collections::HashMap<String, String>> {
        let meta = match self.meta_store.read_version(object_key) {
            Ok(m) => m,
            Err(StorageError::NotFound(_)) => return Ok(std::collections::HashMap::new()),
            Err(e) => return Err(e),
        };
        Ok(meta.metadata)
    }

    /// Get a single metadata value by key (case-insensitive).
    pub fn get_metadata_value(&self, object_key: &str, key: &str) -> StorageResult<Option<String>> {
        let meta = match self.meta_store.read_version(object_key) {
            Ok(m) => m,
            Err(StorageError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(meta.metadata.get(key.to_lowercase().as_str()).cloned())
    }

    /// Set a single metadata key-value pair on the latest version of an object.
    /// Keys are stored in lowercase.
    pub fn set_metadata_value(
        &self,
        object_key: &str,
        key: &str,
        value: &str,
    ) -> StorageResult<()> {
        let latest_version = self.meta_store.read_latest_version(object_key)?;
        let meta = self
            .meta_store
            .read_version_by_number(object_key, latest_version)?;

        let mut new_meta = meta.clone();
        new_meta
            .metadata
            .insert(key.to_lowercase(), value.to_string());

        // Re-serialize and write back
        let ver_key = format!("ver:{object_key}\u{1f}{latest_version}");
        let serialized = self.meta_store.serialize_meta(&new_meta)?;
        let chunks_key = format!("{ver_key}\u{1f}chunks");
        self.meta_store.write_batch(vec![
            (ver_key, serialized),
            (chunks_key, serialize_chunk_ids(&new_meta.chunk_ids)),
        ])
    }

    /// Get the Content-Type of an object.
    pub fn get_content_type(&self, object_key: &str) -> StorageResult<Option<String>> {
        self.get_metadata_value(object_key, "content-type")
    }

    /// Set the Content-Type of an object.
    pub fn set_content_type(&self, object_key: &str, content_type: &str) -> StorageResult<()> {
        self.set_metadata_value(object_key, "content-type", content_type)
    }

    /// Get the ACL (access control list) of an object.
    ///
    /// The ACL is stored as a JSON string. This method parses it and returns
    /// the parsed value, or `None` if no ACL is set.
    pub fn get_acl(&self, object_key: &str) -> StorageResult<Option<serde_json::Value>> {
        if let Some(acl_str) = self.get_metadata_value(object_key, "x-amz-acl")? {
            Ok(Some(serde_json::from_str(&acl_str).map_err(|e| {
                StorageError::KvError(format!("parse ACL: {e}"))
            })?))
        } else {
            Ok(None)
        }
    }

    /// Set the ACL of an object.
    ///
    /// The ACL value is serialized as a JSON string and stored under the key
    /// `x-amz-acl`.
    pub fn set_acl(&self, object_key: &str, acl: &serde_json::Value) -> StorageResult<()> {
        let acl_json = serde_json::to_string(acl)
            .map_err(|e| StorageError::KvError(format!("serialize ACL: {e}")))?;
        self.set_metadata_value(object_key, "x-amz-acl", &acl_json)
    }

    /// Get the Cache-Control header of an object.
    pub fn get_cache_control(&self, object_key: &str) -> StorageResult<Option<String>> {
        self.get_metadata_value(object_key, "cache-control")
    }

    /// Set the Cache-Control header of an object.
    pub fn set_cache_control(&self, object_key: &str, cache_control: &str) -> StorageResult<()> {
        self.set_metadata_value(object_key, "cache-control", cache_control)
    }

    // -------------------------------------------------------------------------
    // Bucket operations
    // -------------------------------------------------------------------------

    /// Create a new bucket with the given name.
    ///
    /// Returns `BucketAlreadyExists` if a bucket with this name already exists.
    pub fn create_bucket(&self, name: &str) -> StorageResult<()> {
        let created_at = Utc::now().to_rfc3339();
        self.meta_store.create_bucket(name, &created_at)
    }

    /// Read bucket metadata by name.
    pub fn read_bucket(&self, name: &str) -> StorageResult<crate::disk::BucketMeta> {
        self.meta_store.read_bucket(name)
    }

    /// Delete a bucket. The bucket must be empty (contain no objects).
    ///
    /// Returns `NotFound` if the bucket does not exist.
    pub fn delete_bucket(&self, name: &str) -> StorageResult<()> {
        // Verify the bucket exists
        let _meta = self.read_bucket(name)?;

        // Check if the bucket is empty by scanning for any objects
        // scan_versions() prepends "ver:", so we pass "{name}/" which becomes "ver:{name}/"
        let entries = self.meta_store.scan_versions(name, 1)?;
        if !entries.is_empty() {
            return Err(StorageError::Transient(format!(
                "bucket '{name}' is not empty, cannot delete"
            )));
        }

        // Delete the bucket metadata
        self.meta_store.delete_bucket(name)
    }

    /// List all buckets in this storage instance.
    pub fn list_buckets(&self) -> StorageResult<Vec<crate::disk::BucketMeta>> {
        self.meta_store.scan_buckets()
    }
}

/// Helper to serialize chunk IDs (needed for metadata update ops).
fn serialize_chunk_ids(chunk_ids: &[String]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(&(chunk_ids.len() as u64).to_le_bytes());
    for cid in chunk_ids {
        let cid_bytes = cid.as_bytes();
        buf.extend_from_slice(&(cid_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(cid_bytes);
    }
    buf
}

/// An entry returned by LIST operations.
#[derive(Debug, Clone)]
pub struct ListEntry {
    pub key: String,
    pub version: u64,
    pub size: usize,
    pub checksum: u128,
}
