/// Configuration for the erasure-coded object storage system.
#[derive(Debug, Clone)]
pub struct Config {
    /// Ordered list of disk paths, one per shard slot.
    /// Each path must exist on a separate physical disk for redundancy.
    pub disk_paths: Vec<String>,
    /// Number of tolerable disk failures (M) for data layer.
    /// Derives: K = M+1 data shards, C = M parity shards, N = 2M+1 total shards.
    pub disk_failures: u32,
    /// Number of metadata replicas (must be <= total_shards). 0 = all disks.
    pub metadata_replicas: usize,
    /// Ordered list of expected disk cluster IDs, one per disk slot.
    /// Empty list means "generate UUIDs for all disks" (fresh cluster).
    /// Non-empty list must match `disk_paths.len()` in length.
    pub disk_uuids: Vec<String>,
}

impl Config {
    /// Returns the number of data shards.
    pub fn data_shards(&self) -> usize {
        self.disk_failures as usize + 1
    }

    /// Returns the number of parity shards.
    pub fn parity_shards(&self) -> usize {
        self.disk_failures as usize
    }

    /// Returns the total number of shards per chunk.
    pub fn total_shards(&self) -> usize {
        self.data_shards() + self.parity_shards()
    }

    /// Returns the effective number of metadata replicas.
    /// 0 means all disks (full replication).
    pub fn effective_replicas(&self) -> usize {
        if self.metadata_replicas == 0 {
            self.total_shards()
        } else {
            std::cmp::min(self.metadata_replicas, self.total_shards())
        }
    }

    /// Validate that the disk path count matches the expected shard count.
    pub fn validate(&self) -> Result<(), String> {
        if self.disk_paths.len() != self.total_shards() {
            return Err(format!(
                "disk_paths count ({}) does not match total_shards ({})",
                self.disk_paths.len(),
                self.total_shards()
            ));
        }
        if !self.disk_uuids.is_empty() && self.disk_uuids.len() != self.total_shards() {
            return Err(format!(
                "disk_uuids count ({}) does not match total_shards ({})",
                self.disk_uuids.len(),
                self.total_shards()
            ));
        }
        Ok(())
    }
}
