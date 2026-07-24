pub mod api;
pub mod apis;
pub mod checksum;
pub mod cluster;
pub mod config;
pub mod disk;
pub mod erasure;
pub mod errors;
pub mod metadata;

pub use api::{ListEntry, ObjectStorage};
pub use config::Config;
pub use disk::{BucketMeta, ChunkStore, Disk, VersionMeta, VersionStatus};
pub use erasure::ErasureCoder;
pub use errors::{StorageError, StorageResult};
pub use metadata::ReplicatedMetaStore;

#[cfg(test)]
mod tests;
