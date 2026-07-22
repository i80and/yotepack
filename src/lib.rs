pub mod cluster;
pub mod config;
pub mod erasure;
pub mod checksum;
pub mod disk;
pub mod metadata;
pub mod api;
pub mod errors;

pub use config::Config;
pub use api::{ObjectStorage, ListEntry};
pub use disk::{ChunkStore, Disk};
pub use erasure::ErasureCoder;
pub use metadata::ReplicatedMetaStore;
pub use errors::{StorageError, StorageResult};

#[cfg(test)]
mod tests;
