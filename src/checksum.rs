/// Checksum utilities using xxHash3 128-bit.
use crate::errors::StorageResult;

/// Compute xxHash3 128-bit checksum over the given data.
pub fn checksum(data: &[u8]) -> u128 {
    xxhash_rust::xxh3::xxh3_128(data)
}

/// Verify data integrity against an expected checksum.
/// Returns Ok(()) if the checksum matches, Err on mismatch.
pub fn verify_checksum(data: &[u8], expected: u128) -> StorageResult<()> {
    let actual = checksum(data);
    if actual == expected {
        Ok(())
    } else {
        Err(crate::errors::StorageError::ChecksumMismatch { expected, actual })
    }
}

// ---------------------------------------------------------------------------
// Incremental (streaming) checksum for large objects
// ---------------------------------------------------------------------------

/// Streaming xxHash3 128-bit hasher.
///
/// Accumulates data via `update()` and produces the same result as
/// `checksum(&[all_data])` via `finalize()`. Uses xxhash-rust's `Xxh3`
/// internal state (256-byte buffer) — negligible overhead.
#[derive(Clone)]
pub struct StreamingChecksum {
    inner: xxhash_rust::xxh3::Xxh3,
}

impl StreamingChecksum {
    /// Create a new streaming hasher with default seed/secret.
    pub fn new() -> Self {
        Self {
            inner: xxhash_rust::xxh3::Xxh3::new(),
        }
    }

    /// Feed more data into the hash.
    pub fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    /// Finalize and return the 128-bit checksum.
    pub fn finalize(self) -> u128 {
        self.inner.digest128()
    }
}

impl Default for StreamingChecksum {
    fn default() -> Self {
        Self::new()
    }
}
