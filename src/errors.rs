/// Error types for the erasure-coded object storage system.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("object not found: {0}")]
    NotFound(String),

    #[error("invalid byte range: {0}")]
    InvalidRange(String),

    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),

    #[error("bucket not found: {0}")]
    BucketNotFound(String),

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

    #[error("IO error")]
    IoError(String),

    #[error("fjall error: {0}")]
    FjallError(String),

    #[error("reed-solomon error: {0}")]
    ReedSolomonError(String),

    #[error("no healthy metadata disks available")]
    NoHealthyDisks,

    #[error("metadata replication failed: {0}")]
    ReplicationFailed(String),

    #[error("cluster ID mismatch on disk {disk_index}: expected={expected}, actual={actual}")]
    ClusterIdMismatch {
        disk_index: usize,
        expected: String,
        actual: String,
    },

    #[error("disk {disk_index} not in cluster: no cluster ID found")]
    DiskIdNotFound { disk_index: usize },
}

impl Clone for StorageError {
    fn clone(&self) -> Self {
        use StorageError::*;
        match self {
            NotFound(s) => NotFound(s.clone()),
            InvalidRange(s) => InvalidRange(s.clone()),
            BucketAlreadyExists(s) => BucketAlreadyExists(s.clone()),
            BucketNotFound(s) => BucketNotFound(s.clone()),
            VersionNotFound { key, version } => VersionNotFound {
                key: key.clone(),
                version: *version,
            },
            VersionConflict => VersionConflict,
            DiskFailed(s) => DiskFailed(s.clone()),
            TooManyFailures => TooManyFailures,
            Transient(s) => Transient(s.clone()),
            ErasureCoding(s) => ErasureCoding(s.clone()),
            ChecksumMismatch { expected, actual } => ChecksumMismatch {
                expected: *expected,
                actual: *actual,
            },
            KvError(s) => KvError(s.clone()),
            IoError(s) => IoError(s.clone()),
            FjallError(s) => FjallError(s.clone()),
            ReedSolomonError(s) => ReedSolomonError(s.clone()),
            NoHealthyDisks => NoHealthyDisks,
            ReplicationFailed(s) => ReplicationFailed(s.clone()),
            ClusterIdMismatch {
                disk_index,
                expected,
                actual,
            } => ClusterIdMismatch {
                disk_index: *disk_index,
                expected: expected.clone(),
                actual: actual.clone(),
            },
            DiskIdNotFound { disk_index } => DiskIdNotFound {
                disk_index: *disk_index,
            },
        }
    }
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

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        StorageError::IoError(e.to_string())
    }
}
