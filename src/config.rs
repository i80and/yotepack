/// Configuration for the erasure-coded object storage system.
#[derive(Debug, Clone)]
pub struct Config {
    /// Base directory for all storage data.
    /// Structure: {base}/disk_{i}/metadata/ for Fjall KV, {base}/disk_{i}/shards/ for data.
    pub base_path: String,
    /// Number of tolerable disk failures (M) for data layer.
    /// Derives: K = M+1 data shards, C = M parity shards, N = 2M+1 total shards.
    pub disk_failures: u32,
    /// Chunk size in bytes (default: 64 MiB).
    pub chunk_size: usize,
    /// Number of metadata replicas (must be <= total_shards). 0 = all disks.
    pub metadata_replicas: usize,
    /// Ordered list of expected disk cluster IDs, one per disk slot.
    /// Empty list means "generate UUIDs for all disks" (fresh cluster).
    /// Non-empty list must match total_shards() in length.
    pub disk_uuids: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            base_path: "./storage".to_string(),
            disk_failures: 1,
            chunk_size: 64 * 1024 * 1024, // 64 MiB
            metadata_replicas: 0, // 0 = all disks
            disk_uuids: Vec::new(),
        }
    }
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
}
