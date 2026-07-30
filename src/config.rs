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
    /// Zstd compression level. None = no compression.
    /// When Some, compression is still gated by MIME type (text-like types compress, binary types don't).
    pub compression_level: Option<i32>,
}

/// MIME types (and prefixes) that are known to be compressible.
/// Already-compressed formats (images, audio, video, archives) are deliberately excluded.
/// Keys are matched as prefix or full MIME type against incoming Content-Type headers.
pub static COMPRESSIBLE_MIME_PREFIXES: &[&str] = &[
    // Text-based formats
    "text/",
    "application/json",
    "application/javascript",
    "application/xml",
    "application/xhtml",
    "application/sql",
    "application/csv",
    "application/x-yaml",
    "application/toml",
    "application/xml-external-parsed-entity",
    "application/manifest+json",
    "application/rtf",
    "application/graphql",
    "application/x-httpd-php",
    "application/x-web-app-manifest+json",
    // Source code
    "application/x-sh",
    "application/x-shellscript",
    "application/x-python",
    "application/x-ruby",
    // Data interchange
    "application/ld+json",
    "application/atom+json",
    "application/hal+json",
    "application/vnd.api+json",
    // Markup
    "text/html",
    "text/css",
    "text/plain",
    "text/markdown",
    "text/xml",
    "text/csv",
    // Mail
    "text/calendar",
    "message/rfc822",
];

/// Check whether a Content-Type should be compressed.
/// Returns true if:
/// 1. `compression_level` is Some (global compression is enabled), AND
/// 2. The content type matches one of the compressible prefixes
pub fn should_compress(content_type: Option<&str>, compression_level: Option<i32>) -> bool {
    let level = match compression_level {
        Some(l) => l,
        None => return false, // Global compression disabled
    };

    // Level 0 means "no compression" even if Some is set
    if level <= 0 {
        return false;
    }

    let mime = match content_type {
        Some(ct) => ct.trim(),
        None => return false, // No content type — don't compress unknowns
    };

    COMPRESSIBLE_MIME_PREFIXES
        .iter()
        .any(|prefix| mime.starts_with(prefix))
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
