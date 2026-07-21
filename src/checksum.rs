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
