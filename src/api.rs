/// Main storage API: PUT, GET, DELETE, LIST, GarbageCollection.
use std::collections::HashSet;

use chrono::Utc;
use futures::io::Cursor;
use futures::AsyncReadExt;

use crate::checksum::{self, StreamingChecksum};
use crate::config::Config;
use crate::disk::{ChunkStore, VersionMeta, VersionStatus};
use crate::errors::{StorageError, StorageResult};

const CHUNK_SIZE_DEFAULT: usize = 64 * 1024 * 1024;

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

        if meta.chunk_ids.is_empty() {
            return Ok(Vec::new());
        }

        let expected_cksum = meta.checksum;
        let total_chunks = meta.chunk_ids.len();
        let data_size = meta.data_size;
        let mut all_data: Vec<u8> = Vec::with_capacity(data_size);

        // Step 3-4: Read and decode each chunk, detecting bitrot
        for (chunk_idx, chunk_id) in meta.chunk_ids.iter().enumerate() {
            let shard_results = self.chunk_store.read_all_shards(chunk_id);

            // Get the expected per-chunk checksum
            let expected_chunk_cksum = meta.chunk_checksums.get(chunk_idx).copied().unwrap_or(0);

            // Calculate expected (unpadded) chunk size for verification
            let is_last_chunk = chunk_idx == total_chunks - 1;
            let expected_chunk_size = if is_last_chunk {
                data_size - (chunk_idx * self.config.chunk_size)
            } else {
                self.config.chunk_size
            };

            // Decode this chunk from its shards (verifies chunk checksum)
            let (chunk_data, corrections) = self.chunk_store.recover_chunk(
                chunk_id,
                expected_chunk_cksum,
                expected_chunk_size,
                &shard_results,
            )?;

            // Trim last chunk if needed (it may have padding from erasure coding)
            let actual_chunk_size = std::cmp::min(chunk_data.len(), expected_chunk_size);
            all_data.extend_from_slice(&chunk_data[..actual_chunk_size]);

            // Write corrections for any failed/corrupted shards
            for (disk_idx, corrected_shard) in corrections {
                let shard_cksum = checksum::checksum(&corrected_shard);
                if let Err(e) =
                    self.chunk_store.disks[disk_idx].write(chunk_id, &corrected_shard, shard_cksum)
                {
                    tracing::warn!("Failed to write correction for chunk {}: {e}", chunk_id);
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
        let chunk_size = self.config.chunk_size;
        let mut buf = vec![0u8; chunk_size];
        let mut chunk_ids: Vec<String> = Vec::new();
        let mut chunk_checksums: Vec<u128> = Vec::new();
        let mut obj_hasher = StreamingChecksum::new();
        let mut data_size: usize = 0;

        // Phase 1: Stream chunks from reader, write each chunk's shards
        loop {
            let n = reader.read(&mut buf[..]).await?;
            if n == 0 {
                break;
            }

            let chunk_data = buf[..n].to_vec();
            let chunk_cksum = checksum::checksum(&chunk_data);

            let id = uuid::Uuid::new_v4().to_string();
            let _disk_cksum = self.chunk_store.write_chunk(&id, &chunk_data, 0)?;

            obj_hasher.update(&chunk_data);
            data_size += n;
            chunk_ids.push(id);
            chunk_checksums.push(chunk_cksum);

            // Restore buffer for next iteration
            buf = vec![0u8; chunk_size];
        }

        // Phase 2: Compute final object-level checksum
        let object_checksum = obj_hasher.finalize();

        if chunk_ids.is_empty() {
            // Empty object: write pending metadata with no chunks, promote
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

        // Phase 3: Check for in-progress write, then set pending
        let next_version = if self.meta_store.has_pending(object_key)? {
            self.meta_store.read_latest_version(object_key)?
        } else {
            self.meta_store.incr_version_counter(object_key)?
        };

        // Set pending in KV store (replicated, with repair-on-write).
        self.meta_store.set_pending(
            object_key,
            next_version,
            &chunk_ids,
            &chunk_checksums,
            object_checksum,
            data_size,
            std::collections::HashMap::new(),
        )?;

        // Phase 4: Promote from pending → committed
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

        let mut latest: std::collections::HashMap<String, VersionMeta> =
            std::collections::HashMap::new();

        for (_key, meta) in entries {
            if meta.status != VersionStatus::Committed {
                continue;
            }
            let object_key = _key.strip_prefix("ver:").unwrap_or(&_key).to_string();
            let existing = latest.entry(object_key).or_insert(meta.clone());
            if meta.version > existing.version {
                *existing = meta;
            }
        }

        let mut results: Vec<_> = latest.into_values().collect();
        results.sort_by_key(|m| m.chunk_ids.first().cloned().unwrap_or_default());

        if let Some(m) = marker {
            results.retain(|e| {
                e.chunk_ids
                    .first()
                    .map(|cid| cid.as_str() > m)
                    .unwrap_or(false)
            });
        }

        results.truncate(limit);

        let mut output = Vec::with_capacity(results.len());
        for meta in &results {
            let size = if meta.chunk_ids.is_empty() {
                0
            } else {
                let cs = if self.config.chunk_size > 0 {
                    self.config.chunk_size
                } else {
                    CHUNK_SIZE_DEFAULT
                };
                meta.chunk_ids.len() * cs
            };

            output.push(ListEntry {
                key: meta.chunk_ids.first().cloned().unwrap_or_default(),
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
        let mut referenced: HashSet<String> = HashSet::new();

        let all_entries = self.meta_store.scan_versions("", usize::MAX)?;
        for (_key, meta) in all_entries {
            if meta.status == VersionStatus::Committed {
                for cid in &meta.chunk_ids {
                    referenced.insert(cid.clone());
                }
            }
        }

        let all_chunks = self.meta_store.scan_chunks()?;
        for (chunk_key, _cksum) in all_chunks {
            let chunk_id = chunk_key.strip_prefix("chk:").unwrap_or(&chunk_key);
            if !referenced.contains(chunk_id) {
                let _ = self.chunk_store.disks[0].delete_chunk(chunk_id);
                let _ = self.meta_store.delete_chunk_meta(chunk_id);
            }
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // StartupRecovery
    // -------------------------------------------------------------------------

    pub fn recover_on_startup(&self) -> StorageResult<()> {
        let all_entries = self.meta_store.scan_versions("", usize::MAX)?;

        for (key, meta) in all_entries {
            if meta.status == VersionStatus::Pending {
                // key format: ver:object_key:version
                // Extract object_key by removing the trailing :version
                let object_key = if let Some(colon) = key.rfind(':') {
                    key[..colon].strip_prefix("ver:").unwrap_or(&key[4..])
                } else {
                    key.strip_prefix("ver:").unwrap_or(&key)
                };

                let mut all_exist = true;
                for chunk_id in &meta.chunk_ids {
                    if self.chunk_store.disks[0].read(chunk_id).is_err() {
                        all_exist = false;
                        break;
                    }
                }

                if all_exist && !meta.chunk_ids.is_empty() {
                    let _ = self.meta_store.promote_version(object_key, meta.version);
                } else {
                    let _ = self.meta_store.delete_pending(object_key, meta.version);
                    tracing::warn!("Abandoned pending version: {object_key}");
                }
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
        let ver_key = format!("ver:{object_key}:{latest_version}");
        let serialized = self.meta_store.serialize_meta(&new_meta)?;
        let chunks_key = format!("{ver_key}:chunks");
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
        let prefix = format!("{name}/");
        let entries = self.meta_store.scan_versions(&prefix, 1)?;
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
