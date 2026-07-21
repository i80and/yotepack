//! Smoke tests: quick, deterministic, verify basic functionality.
//!
//! These tests should run on every commit and complete in < 1 second each.
//! They verify the happy path and obvious failure modes.

use std::sync::OnceLock;

use crate::disk::VersionStatus;
use crate::StorageError;
use crate::tests::support;
use crate::ObjectStorage;

/// Reusable tokio runtime for blocking async operations.
fn rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

// ---------------------------------------------------------------------------
// Basic CRUD
// ---------------------------------------------------------------------------

#[test]
fn smoke_put_get_basic() {
    let tmp = support::test_dir("smoke_put_get_basic");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let data = b"Hello, erasure-coded world!";
    let token = rt().block_on(storage.put("test-key", data)).unwrap();
    assert!(token > 0);

    let result = rt().block_on(storage.get("test-key", Some(token))).unwrap();
    assert_eq!(result, data);
}

#[test]
fn smoke_put_get_multiple_objects() {
    let tmp = support::test_dir("smoke_put_get_many");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // Write 10 objects
    let tokens: Vec<u64> = (0..10)
        .map(|i| {
            let data = format!("object-{i}").into_bytes();
            rt().block_on(storage.put(&format!("key-{i}"), &data)).unwrap()
        })
        .collect();

    // Read all back with tokens
    for (i, token) in tokens.iter().enumerate() {
        let result = rt().block_on(storage.get(&format!("key-{i}"), Some(*token))).unwrap();
        assert_eq!(result, format!("object-{i}").into_bytes());
    }
}

#[test]
fn smoke_put_get_no_token() {
    let tmp = support::test_dir("smoke_put_get_no_token");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    rt().block_on(storage.put("key", b"data")).unwrap();

    // Read without token (read-committed)
    let result = rt().block_on(storage.get("key", None)).unwrap();
    assert_eq!(result, b"data");
}

#[test]
fn smoke_read_latest_version() {
    let tmp = support::test_dir("smoke_read_latest");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let _v1 = rt().block_on(storage.put("key", b"v1")).unwrap();
    let v2 = rt().block_on(storage.put("key", b"v2")).unwrap();

    // read_latest_version should return the newest
    let latest = storage.meta_store.read_latest_version("key").unwrap();
    assert_eq!(latest, v2);

    // read_version should return latest committed metadata
    let meta = storage.meta_store.read_version("key").unwrap();
    assert_eq!(meta.version, v2);
    assert_eq!(meta.status, VersionStatus::Committed);
}

// ---------------------------------------------------------------------------
// Versioning
// ---------------------------------------------------------------------------

#[test]
fn smoke_version_concurrency() {
    let tmp = support::test_dir("smoke_version_concurrency");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // Two PUTs to same key — one should win
    let v1 = rt().block_on(storage.put("key", b"first")).unwrap();
    let v2 = rt().block_on(storage.put("key", b"second")).unwrap();

    assert!(v2 > v1);

    // Latest read should see v2
    let result = rt().block_on(storage.get("key", None)).unwrap();
    assert_eq!(result, b"second");

    // Token for v1 should still work (read-your-writes)
    let result_v1 = rt().block_on(storage.get("key", Some(v1))).unwrap();
    assert_eq!(result_v1, b"first");
}

#[test]
fn smoke_version_monotonicity() {
    let tmp = support::test_dir("smoke_version_mono");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // Monotonicity is per-object: write to same key repeatedly
    let mut prev_version = 0u64;
    for i in 0..5 {
        let data = format!("version-{i}").into_bytes();
        let version = rt().block_on(storage.put("mono-key", &data)).unwrap();
        assert!(version > prev_version, "version {version} should be > {prev_version}");
        prev_version = version;
    }
}

#[test]
fn smoke_delete() {
    let tmp = support::test_dir("smoke_delete");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    rt().block_on(storage.put("key", b"data")).unwrap();

    // Delete
    storage.delete("key").unwrap();

    // After delete, reading should fail
    let result = rt().block_on(storage.get("key", None));
    assert!(result.is_err());
    assert!(matches!(result, Err(StorageError::NotFound(_))));
}

#[test]
fn smoke_delete_nonexistent() {
    let tmp = support::test_dir("smoke_delete_missing");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let result = storage.delete("nonexistent");
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// LIST
// ---------------------------------------------------------------------------

#[test]
fn smoke_list_basic() {
    let tmp = support::test_dir("smoke_list_basic");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    rt().block_on(storage.put("obj-a", b"data-a")).unwrap();
    rt().block_on(storage.put("obj-b", b"data-b")).unwrap();
    rt().block_on(storage.put("other", b"data-other")).unwrap();

    let entries = storage.list("", None, 100).unwrap();
    assert_eq!(entries.len(), 3);

    // Verify all entries have positive version and size
    for entry in &entries {
        assert!(entry.version > 0);
        assert!(entry.size > 0);
        assert!(!entry.key.is_empty());
    }
}

#[test]
fn smoke_list_prefix_filter() {
    let tmp = support::test_dir("smoke_list_prefix");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    rt().block_on(storage.put("alpha-1", b"data")).unwrap();
    rt().block_on(storage.put("alpha-2", b"data")).unwrap();
    rt().block_on(storage.put("beta-1", b"data")).unwrap();

    let entries = storage.list("alpha", None, 100).unwrap();
    assert_eq!(entries.len(), 2);
}

#[test]
fn smoke_list_limit() {
    let tmp = support::test_dir("smoke_list_limit");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    for i in 0..10 {
        rt().block_on(storage.put(&format!("obj-{i}"), b"data")).unwrap();
    }

    let entries = storage.list("", None, 3).unwrap();
    assert!(entries.len() <= 3);
}

// ---------------------------------------------------------------------------
// Large objects (multi-chunk)
// ---------------------------------------------------------------------------

#[test]
fn smoke_large_object() {
    let tmp = support::test_dir("smoke_large_obj");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // Data larger than chunk size (1 KiB) to trigger multiple chunks
    let data: Vec<u8> = (0..8192).map(|i| i as u8).collect();
    let token = rt().block_on(storage.put("large", &data)).unwrap();
    let result = rt().block_on(storage.get("large", Some(token))).unwrap();
    assert_eq!(result, data);
}

#[test]
fn smoke_exact_chunk_boundary() {
    let tmp = support::test_dir("smoke_chunk_boundary");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // Exactly one chunk size
    let data = vec![42u8; 1024];
    let token = rt().block_on(storage.put("exact", &data)).unwrap();
    let result = rt().block_on(storage.get("exact", Some(token))).unwrap();
    assert_eq!(result, data);
}

#[test]
fn smoke_just_over_chunk_boundary() {
    let tmp = support::test_dir("smoke_over_boundary");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // Just over one chunk — triggers two chunks
    let data = vec![42u8; 1025];
    let token = rt().block_on(storage.put("over", &data)).unwrap();
    let result = rt().block_on(storage.get("over", Some(token))).unwrap();
    assert_eq!(result, data);
}

// ---------------------------------------------------------------------------
// Metadata layer
// ---------------------------------------------------------------------------

#[test]
fn smoke_meta_disk_count() {
    let tmp = support::test_dir("smoke_meta_count");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // With M=1: total_shards = 3, so 3 metadata disks
    assert_eq!(storage.meta_store.total_disk_count(), 3);
    assert_eq!(storage.meta_store.healthy_disk_count(), 3);
    assert!(storage.meta_store.is_safe());
}

#[test]
fn smoke_meta_persist() {
    let tmp = support::test_dir("smoke_persist");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    rt().block_on(storage.put("persist-key", b"persist-data")).unwrap();

    // Persist all metadata databases (verify it doesn't error)
    storage.meta_store.persist().unwrap();
}

// ---------------------------------------------------------------------------
// Garbage collection
// ---------------------------------------------------------------------------

#[test]
fn smoke_gc_removes_unreferenced() {
    let tmp = support::test_dir("smoke_gc");
    let config = support::make_test_config(&tmp, 1, 1024, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // Write, delete, then GC
    rt().block_on(storage.put("gc-key", b"data")).unwrap();
    storage.delete("gc-key").unwrap();
    storage.garbage_collect().unwrap();

    // Object should be gone after GC
    let entries = storage.list("", None, 100).unwrap();
    assert_eq!(entries.len(), 0);
}

// ---------------------------------------------------------------------------
// Startup recovery
// ---------------------------------------------------------------------------

#[test]
fn smoke_startup_recovery() {
    let tmp = support::test_dir("smoke_recovery");
    let config = support::make_test_config(&tmp, 1, 1024, 0);

    // Write and commit an object
    {
        let storage = ObjectStorage::new(config.clone()).unwrap();
        rt().block_on(storage.put("recover-key", b"recover-data")).unwrap();
    }

    // Reopen — data should survive
    let storage = ObjectStorage::new(config).unwrap();
    let result = rt().block_on(storage.get("recover-key", None)).unwrap();
    assert_eq!(result, b"recover-data");
}
