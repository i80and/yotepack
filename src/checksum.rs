/// Checksum utilities.
///
/// Two hash functions serve different purposes:
/// - `per_chunk_checksum`: fast xxHash3 for per-chunk bitrot detection
/// - `object_hash`: Blake3 for object-level ETag (collision-resistant)
use crate::errors::StorageResult;

/// Fast per-chunk xxHash3-128 checksum.
///
/// Used for bitrot detection and erasure-coding repair verification.
/// Speed matters; collision resistance is not required.
pub fn per_chunk_checksum(data: &[u8]) -> u128 {
    xxhash_rust::xxh3::xxh3_128(data)
}

/// Verify chunk data integrity against an expected per-chunk checksum.
pub fn verify_chunk_checksum(data: &[u8], expected: u128) -> StorageResult<()> {
    let actual = per_chunk_checksum(data);
    if actual == expected {
        Ok(())
    } else {
        Err(crate::errors::StorageError::ChecksumMismatch { expected, actual })
    }
}

/// Blake3-128 content hash for object-level ETag.
///
/// Cryptographically collision-resistant, making ETags meaningful.
/// The hash covers the raw, decoded object data (all chunks concatenated).
pub fn object_hash(data: &[u8]) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(data);
    let hash = hasher.finalize();
    u128::from_le_bytes(hash.as_bytes()[..16].try_into().unwrap())
}

/// Verify object data integrity against an expected content hash.
pub fn verify_object_hash(data: &[u8], expected: u128) -> StorageResult<()> {
    let actual = object_hash(data);
    if actual == expected {
        Ok(())
    } else {
        Err(crate::errors::StorageError::ChecksumMismatch { expected, actual })
    }
}

// ---------------------------------------------------------------------------
// Incremental (streaming) content hash for large objects (ETag)
// ---------------------------------------------------------------------------

/// Streaming Blake3 hasher for object-level ETag computation.
///
/// Accumulates data via `update()` and produces the same result as
/// `object_hash(&[all_data])` via `finalize()`.
#[derive(Clone)]
pub struct StreamingObjectHash {
    hasher: blake3::Hasher,
}

impl StreamingObjectHash {
    /// Create a new streaming hasher.
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
        }
    }

    /// Feed more data into the hash.
    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    /// Finalize and return the 128-bit content hash.
    pub fn finalize(self) -> u128 {
        let hash = self.hasher.finalize();
        u128::from_le_bytes(hash.as_bytes()[..16].try_into().unwrap())
    }
}

impl Default for StreamingObjectHash {
    fn default() -> Self {
        Self::new()
    }
}

// Keep a type alias for backward compat — maps to the streaming object hash
pub type StreamingChecksum = StreamingObjectHash;
