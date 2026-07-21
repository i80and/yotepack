/// Error types for the erasure-coded object storage system.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("object not found: {0}")]
    NotFound(String),

    #[error("version not found: key={key}, version={version}")]
    VersionNotFound { key: String, version: u64 },

    #[error("version conflict: another version is already committed")]
    VersionConflict,

    #[error("disk failed: {0}")]
    DiskFailed(String),

    #[error("too many disk failures for chunk, cannot recover")]
    TooManyFailures,

    #[error("transient I/O error: {0}")]
    Transient(String),

    #[error("erasure coding error: {0}")]
    ErasureCoding(String),

    #[error("checksum mismatch: expected={expected}, actual={actual}")]
    ChecksumMismatch { expected: u128, actual: u128 },

    #[error("KV store error: {0}")]
    KvError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("fjall error: {0}")]
    FjallError(String),

    #[error("reed-solomon error: {0}")]
    ReedSolomonError(String),

    #[error("no healthy metadata disks available")]
    NoHealthyDisks,

    #[error("metadata replication failed: {0}")]
    ReplicationFailed(String),
}

/// Result type for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;

impl From<fjall::Error> for StorageError {
    fn from(e: fjall::Error) -> Self {
        StorageError::FjallError(e.to_string())
    }
}

impl From<reed_solomon_simd::Error> for StorageError {
    fn from(e: reed_solomon_simd::Error) -> Self {
        StorageError::ReedSolomonError(e.to_string())
    }
}
