//! Property-based fuzzing tests for storage correctness.
//!
//! Uses proptest to generate random inputs and verify invariants:
//! - Read-after-write: `PUT + GET(token) == data`
//! - Version monotonicity: `GET().version >= previous_version`
//! - No ghost data: every chunk in scan_chunks() is referenced
//! - Checksum consistency: `checksum(GET(key)) == meta.checksum`

use proptest::prelude::*;
use std::collections::HashSet;
use std::sync::OnceLock;

use crate::disk::VersionStatus;
use crate::tests::support;
use crate::{ObjectStorage, StorageError};

/// Reusable tokio runtime for blocking async operations inside proptest.
fn rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generate random non-empty data (1-4 KiB).
fn any_data() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any_u8(), 1..4096)
}

/// Generate a random u8 for data generation.
fn any_u8() -> impl Strategy<Value = u8> {
    any::<u8>()
}

/// Generate a random object key (4-8 alphanumeric chars).
fn any_key() -> impl Strategy<Value = String> {
    let letters = "abcdefghijklmnopqrstuvwxyz0123456789";
    proptest::collection::vec(
        any::<usize>().prop_map(|v| letters.as_bytes()[v % letters.len()] as char),
        4..8,
    )
    .prop_map(|chars| chars.into_iter().collect())
}

// ---------------------------------------------------------------------------
// Invariant 1: Read-after-write
// ---------------------------------------------------------------------------

#[test]
fn prop_read_after_write() {
    proptest!(|(data in any_data())| {
        let tmp = support::test_dir("fuzz_rainw");
        let config = support::make_test_config(&tmp, 1, 1024, 0);
        let storage = ObjectStorage::new(config).unwrap();

        let key = "rainw-key";
        let token = rt().block_on(storage.put(key, &data)).unwrap();
        assert!(token > 0);

        let result = rt().block_on(storage.get(key, Some(token))).unwrap();
        assert_eq!(result, data);
    });
}

#[test]
fn prop_read_after_write_many_keys() {
    proptest!(|(keys in proptest::collection::vec(any_key(), 1..20))| {
        let tmp = support::test_dir("fuzz_rainw_many");
        let config = support::make_test_config(&tmp, 1, 1024, 0);
        let storage = ObjectStorage::new(config).unwrap();

        let tokens: Vec<u64> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let data = format!("data-{i}").into_bytes();
                rt().block_on(storage.put(k, &data))
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        for (i, (k, token)) in keys.iter().zip(tokens.iter()).enumerate() {
            let data = format!("data-{i}").into_bytes();
            let result = rt().block_on(storage.get(k, Some(*token))).unwrap();
            assert_eq!(result, data, "key={k}");
        }
    });
}

// ---------------------------------------------------------------------------
// Invariant 2: Version monotonicity (per-object)
// ---------------------------------------------------------------------------

#[test]
fn prop_version_monotonicity() {
    proptest!(|(count in 1usize..50usize)| {
        let tmp = support::test_dir("fuzz_monotonicity");
        let config = support::make_test_config(&tmp, 1, 1024, 0);
        let storage = ObjectStorage::new(config).unwrap();

        // Monotonicity is per-object: write to same key repeatedly
        let mut prev_version = 0u64;
        for _i in 0..count {
            let data = vec![prev_version as u8; 64];
            let version = rt().block_on(storage.put("mono-key", &data)).unwrap();
            assert!(
                version > prev_version,
                "version {version} should be > {prev_version}"
            );
            prev_version = version;
        }
    });
}

// ---------------------------------------------------------------------------
// Invariant 3: No ghost data (every chunk is referenced)
// ---------------------------------------------------------------------------

#[test]
fn prop_no_ghost_data() {
    proptest!(|(count in 1usize..30usize)| {
        let tmp = support::test_dir("fuzz_no_ghost");
        let config = support::make_test_config(&tmp, 1, 1024, 0);
        let storage = ObjectStorage::new(config).unwrap();

        // Write some objects
        let mut chunk_set: HashSet<String> = HashSet::new();
        for i in 0..count {
            let data = vec![i as u8; 64 + (i % 10) * 64];
            let token = rt().block_on(storage.put(&format!("ghost-{i}"), &data)).unwrap();
            let meta = storage.meta_store.read_version_by_number(&format!("ghost-{i}"), token).unwrap();
            for cid in &meta.chunk_ids {
                chunk_set.insert(cid.clone());
            }
        }

        // Scan all chunks from metadata
        let scanned_chunks = storage.meta_store.scan_chunks().unwrap();
        let scanned_ids: HashSet<String> = scanned_chunks
            .into_iter()
            .map(|(k, _)| k.strip_prefix("chk:").unwrap_or(&k).to_string())
            .collect();

        // All scanned chunks should be referenced
        for cid in &scanned_ids {
            assert!(
                chunk_set.contains(cid),
                "chunk {cid} is in scan_chunks but not referenced by any committed version (ghost data)"
            );
        }
    });
}

// ---------------------------------------------------------------------------
// Invariant 4: Checksum consistency
// ---------------------------------------------------------------------------

#[test]
fn prop_checksum_consistency() {
    proptest!(|(count in 1usize..20usize)| {
        let tmp = support::test_dir("fuzz_checksum");
        let config = support::make_test_config(&tmp, 1, 1024, 0);
        let storage = ObjectStorage::new(config).unwrap();

        for i in 0..count {
            // Use varying data to exercise checksum across different chunk sizes
            let data: Vec<u8> = (0..(64 + i * 32))
                .map(|j| ((i * 7 + j * 13) % 256) as u8)
                .collect();
            let token = rt().block_on(storage.put(&format!("cksum-{i}"), &data)).unwrap();
            let meta = storage.meta_store.read_version_by_number(&format!("cksum-{i}"), token).unwrap();

            // Read data back
            let result = rt().block_on(storage.get(&format!("cksum-{i}"), Some(token))).unwrap();
            assert_eq!(result, data, "data mismatch for cksum-{i}");

            // Checksum should match
            let actual_cksum = xxhash_rust::xxh3::xxh3_128(&result);
            assert_eq!(
                meta.checksum, actual_cksum,
                "checksum mismatch for cksum-{i}: meta={:#x}, actual={:#x}",
                meta.checksum, actual_cksum
            );
        }
    });
}

// ---------------------------------------------------------------------------
// Invariant 5: No ghost data after delete and GC
// ---------------------------------------------------------------------------

#[test]
fn prop_no_ghost_data_after_gc() {
    proptest!(|(count in 5usize..20usize)| {
        let tmp = support::test_dir("fuzz_gc_ghost");
        let config = support::make_test_config(&tmp, 1, 1024, 0);
        let storage = ObjectStorage::new(config).unwrap();

        // Write and delete objects
        let mut chunk_set: HashSet<String> = HashSet::new();
        for i in 0..count {
            let data = vec![i as u8; 64];
            let token = rt().block_on(storage.put(&format!("gc-ghost-{i}"), &data)).unwrap();
            let meta = storage.meta_store.read_version_by_number(&format!("gc-ghost-{i}"), token).unwrap();
            for cid in &meta.chunk_ids {
                chunk_set.insert(cid.clone());
            }
            storage.delete(&format!("gc-ghost-{i}")).unwrap();
        }

        // GC should remove unreferenced chunks
        storage.garbage_collect().unwrap();

        // Scan chunks after GC
        let scanned_chunks = storage.meta_store.scan_chunks().unwrap();
        let scanned_ids: HashSet<String> = scanned_chunks
            .into_iter()
            .map(|(k, _)| k.strip_prefix("chk:").unwrap_or(&k).to_string())
            .collect();

        // After GC, no ghost chunks should remain
        // (since all objects were deleted, scanned_ids should be empty)
        assert!(
            scanned_ids.is_empty(),
            "found {} ghost chunks after GC",
            scanned_ids.len()
        );
    });
}

// ---------------------------------------------------------------------------
// Invariant 6: Version conflict is resolved
// ---------------------------------------------------------------------------

#[test]
fn prop_version_conflict_resolved() {
    proptest!(|(count in 1usize..30usize)| {
        let tmp = support::test_dir("fuzz_conflict");
        let config = support::make_test_config(&tmp, 1, 1024, 0);
        let storage = ObjectStorage::new(config).unwrap();

        // Write N versions to the same key (simulates competing writers)
        let mut versions: Vec<u64> = Vec::new();
        for i in 0..count {
            let data = format!("v{i}").into_bytes();
            match rt().block_on(storage.put("conflict-key", &data)) {
                Ok(token) => versions.push(token),
                Err(StorageError::VersionConflict) => {
                    // Another write won — verify the version we get is correct
                    let meta = storage.meta_store.read_version("conflict-key").unwrap();
                    assert!(
                        meta.version > 0
                            && meta.status == VersionStatus::Committed
                    );
                    // The latest version should be readable
                    let result = rt().block_on(storage.get("conflict-key", None)).unwrap();
                    assert!(!result.is_empty());
                    break;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        // At least one version should have been written
        assert!(!versions.is_empty());

        // Final state: one committed version
        let latest = storage.meta_store.read_latest_version("conflict-key").unwrap();
        assert!(latest > 0);

        let latest_meta = storage.meta_store.read_version("conflict-key").unwrap();
        assert_eq!(latest_meta.status, VersionStatus::Committed);
    });
}

// ---------------------------------------------------------------------------
// Invariant 7: List returns consistent entries
// ---------------------------------------------------------------------------

#[test]
fn prop_list_consistency() {
    proptest!(|(count in 1usize..30usize)| {
        let tmp = support::test_dir("fuzz_list");
        let config = support::make_test_config(&tmp, 1, 1024, 0);
        let storage = ObjectStorage::new(config).unwrap();

        // Track object keys and their tokens
        let mut objects: Vec<(String, u64)> = Vec::new();
        for i in 0..count {
            let data = vec![i as u8; 32 + (i % 5) * 32];
            let key = format!("list-key-{i}");
            let token = rt().block_on(storage.put(&key, &data)).unwrap();
            objects.push((key, token));
        }

        let entries = storage.list("", None, 100).unwrap();

        // Every entry should have positive version and size
        for entry in &entries {
            assert!(entry.version > 0, "list entry has version 0: {entry:?}");
            assert!(entry.size > 0, "list entry has size 0: {entry:?}");
        }

        // Number of entries should match number of objects written
        assert_eq!(entries.len(), count);

        // All listed entries should be readable (use token-based read)
        for (key, token) in &objects {
            let result = rt().block_on(storage.get(key, Some(*token))).unwrap();
            assert!(!result.is_empty());
        }
    });
}

// ---------------------------------------------------------------------------
// Invariant 8: Read-your-writes with multiple version bumps
// ---------------------------------------------------------------------------

#[test]
fn prop_read_your_writes_multiple_versions() {
    proptest!(|(versions in 1usize..20usize)| {
        let tmp = support::test_dir("fuzz_rainw_multi");
        let config = support::make_test_config(&tmp, 1, 1024, 0);
        let storage = ObjectStorage::new(config).unwrap();

        let mut tokens: Vec<u64> = Vec::new();
        for i in 0..versions {
            let data = format!("v{i}").into_bytes();
            let token = rt().block_on(storage.put("multi-v", &data)).unwrap();
            tokens.push(token);

            // Read-with-token must return the exact data we just wrote
            let result = rt().block_on(storage.get("multi-v", Some(token))).unwrap();
            assert_eq!(result, data, "read-your-writes failed at version {i}");
        }

        // All versions should be readable with their tokens
        for (i, token) in tokens.iter().enumerate() {
            let data = format!("v{i}").into_bytes();
            let result = rt().block_on(storage.get("multi-v", Some(*token))).unwrap();
            assert_eq!(result, data, "re-read of version {i} failed");
        }
    });
}

// ---------------------------------------------------------------------------
// Invariant 9: Multi-chunk read-after-write
// ---------------------------------------------------------------------------

#[test]
fn prop_read_after_write_multi_chunk() {
    proptest!(|(size in 2048..8192)| {
        let tmp = support::test_dir("fuzz_rainw_multichunk");
        let config = support::make_test_config(&tmp, 1, 1024, 0);
        let storage = ObjectStorage::new(config).unwrap();

        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let token = rt().block_on(storage.put("multi-chunk-key", &data)).unwrap();

        // Read with token
        let result = rt().block_on(storage.get("multi-chunk-key", Some(token))).unwrap();
        assert_eq!(result, data);

        // Read without token (should see latest)
        let result2 = rt().block_on(storage.get("multi-chunk-key", None)).unwrap();
        assert_eq!(result2, data);
    });
}

// ---------------------------------------------------------------------------
// Invariant 10: CRUD lifecycle
// ---------------------------------------------------------------------------

#[test]
fn prop_crud_lifecycle() {
    proptest!(|(count in 5usize..30usize)| {
        let tmp = support::test_dir("fuzz_crud");
        let config = support::make_test_config(&tmp, 1, 1024, 0);
        let storage = ObjectStorage::new(config).unwrap();

        // Track object keys and their tokens
        let mut objects: Vec<(String, u64)> = Vec::new();
        for i in 0..count {
            let key = format!("crud-{i}");
            let data = vec![i as u8; 32];

            // PUT
            let token = rt().block_on(storage.put(&key, &data)).unwrap();
            objects.push((key.clone(), token));

            // GET with token
            let result = rt().block_on(storage.get(&key, Some(token))).unwrap();
            assert_eq!(result, data);

            // GET without token
            let result_nc = rt().block_on(storage.get(&key, None)).unwrap();
            assert_eq!(result_nc, data);

            // LIST should include count entries at minimum
            let entries = storage.list("", None, 100).unwrap();
            assert_eq!(entries.len(), i + 1, "LIST should have {} entries after {} objects", i + 1, i + 1);
        }

        // Delete half the keys
        let to_delete: Vec<String> = objects.iter().take(count / 2).map(|(k, _)| k.clone()).collect();
        for key in &to_delete {
            storage.delete(key).unwrap();
            assert!(rt().block_on(storage.get(key, None)).is_err());
        }

        // LIST should now have remaining entries
        let remaining = count - to_delete.len();
        let entries = storage.list("", None, 100).unwrap();
        assert_eq!(entries.len(), remaining);

        // Remaining objects should still be readable
        for (key, token) in objects.iter().skip(remaining) {
            let result = rt().block_on(storage.get(key, Some(*token))).unwrap();
            assert!(!result.is_empty());
        }
    });
}

// ---------------------------------------------------------------------------
// Invariant 11: Random operation sequences
// ---------------------------------------------------------------------------

/// Enum of operations that can be applied to storage.
#[derive(Debug, Clone)]
enum FuzzOp {
    Put { key: String, data: Vec<u8> },
    Get { key: String, token: Option<u64> },
    Delete { key: String },
}

fn any_op() -> impl Strategy<Value = FuzzOp> {
    proptest::collection::vec(any::<u8>(), 0..50)
        .prop_map(|bytes| {
            let first = bytes.first().copied().unwrap_or(0);
            let key_len = ((first % 4) as usize) + 2; // 2-5 chars
            let key: String = (0..key_len)
                .map(|i| bytes.get(i + 1).copied().unwrap_or(b'a') as char)
                .collect();

            match first % 3 {
                0 => {
                    let data_start = 1 + key_len;
                    let data = if data_start < bytes.len() {
                        bytes[data_start..].to_vec()
                    } else {
                        vec![0]
                    };
                    FuzzOp::Put { key, data }
                }
                1 => {
                    let token = if 1 + key_len < bytes.len() {
                        let token_val = bytes[1 + key_len] as u64;
                        if token_val > 0 { Some(token_val) } else { None }
                    } else {
                        None
                    };
                    FuzzOp::Get { key, token }
                }
                _ => FuzzOp::Delete { key },
            }
        })
}

#[test]
fn prop_random_operation_sequence() {
    proptest!(|(ops in proptest::collection::vec(any_op(), 1..50))| {
        let tmp = support::test_dir("fuzz_ops");
        let config = support::make_test_config(&tmp, 1, 1024, 0);
        let storage = ObjectStorage::new(config).unwrap();

        for op in ops {
            match op {
                FuzzOp::Put { key, data } => {
                    let result = rt().block_on(storage.put(&key, &data));
                    // PUT may succeed or fail (conflict), both are valid
                    let _ = result;
                }
                FuzzOp::Get { key, token } => {
                    let result = rt().block_on(storage.get(&key, token));
                    // GET may succeed (object exists) or fail (not found), both valid
                    let _ = result;
                }
                FuzzOp::Delete { key } => {
                    let result = storage.delete(&key);
                    // DELETE may succeed (object exists) or fail (not found), both valid
                    let _ = result;
                }
            }
        }

        // Final invariant: all listed entries have valid versions
        let entries = storage.list("", None, 100).unwrap();
        for entry in &entries {
            assert!(entry.version > 0, "list entry has version 0: {entry:?}");
            assert!(entry.size > 0, "list entry has size 0: {entry:?}");
        }
    });
}
