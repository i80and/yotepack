//! Tests for last_modified time tracking.
//!
//! Verifies that:
//! - Objects get a last_modified timestamp on commit
//! - The timestamp is RFC 3339 compliant and in UTC
//! - The timestamp persists across restarts
//! - Updating object metadata updates last_modified
//! - Multiple versions each have their own last_modified

use std::time::Duration;

use crate::disk::VersionStatus;
use crate::tests::support;
use crate::ObjectStorage;

/// Reusable tokio runtime for blocking async operations.
fn rt() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

#[test]
fn test_last_modified_is_set_on_put() {
    let tmp = support::test_dir("test_last_modified_set");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    rt().block_on(storage.put("key", b"data")).unwrap();

    // Read version metadata — should have a non-empty last_modified
    let meta = storage.meta_store.read_version("key").unwrap();
    assert!(
        !meta.last_modified.is_empty(),
        "last_modified should be set"
    );

    // Should be valid RFC 3339
    let parsed = chrono::DateTime::parse_from_rfc3339(&meta.last_modified)
        .expect("last_modified should be valid RFC 3339");

    // Should be in UTC (offset should be +00:00)
    assert!(
        parsed.offset().to_string().contains("+00:00")
            || parsed.offset().to_string().contains("UTC"),
        "last_modified should be in UTC: {}",
        parsed.offset()
    );

    // Should be reasonably recent (within the last 10 seconds)
    let now = chrono::Utc::now();
    let diff = (now.naive_utc() - parsed.naive_utc())
        .num_seconds()
        .unsigned_abs();
    assert!(diff < 10, "last_modified should be recent: diff={diff}s");
}

#[test]
fn test_last_modified_empty_object() {
    let tmp = support::test_dir("test_last_modified_empty");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    rt().block_on(storage.put("empty-key", b"")).unwrap();

    let meta = storage.meta_store.read_version("empty-key").unwrap();
    assert!(
        !meta.last_modified.is_empty(),
        "last_modified should be set for empty objects"
    );
    let _ = chrono::DateTime::parse_from_rfc3339(&meta.last_modified)
        .expect("last_modified should be valid RFC 3339");
}

#[test]
fn test_last_modified_persists() {
    let tmp = support::test_dir("test_last_modified_persist");
    let config = support::make_test_config(&tmp, 1, 0);

    // Write and commit an object, then drop the storage
    let time1: String = {
        let storage = ObjectStorage::new(config.clone()).unwrap();
        rt().block_on(storage.put("persist-key", b"persist-data"))
            .unwrap();
        let meta1 = storage.meta_store.read_version("persist-key").unwrap();
        let time = meta1.last_modified.clone();

        // Persist to make sure it's on disk
        storage.meta_store.persist().unwrap();

        // Drop the storage so we can reopen
        drop(storage);
        time
    };

    // Reopen the storage
    let storage = ObjectStorage::new(config).unwrap();

    // The last_modified should be the same after reopening
    let meta2 = storage.meta_store.read_version("persist-key").unwrap();
    assert_eq!(
        time1, meta2.last_modified,
        "last_modified should persist across restarts"
    );
}

#[test]
fn test_last_modified_new_version_different() {
    let tmp = support::test_dir("test_last_modified_version_diff");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // First version
    let v1 = rt().block_on(storage.put("key", b"v1")).unwrap();
    let meta1 = storage.meta_store.read_version("key").unwrap();
    assert_eq!(meta1.version, v1);
    let time1 = meta1.last_modified.clone();

    // Small delay, then second version
    std::thread::sleep(Duration::from_millis(50));

    let v2 = rt().block_on(storage.put("key", b"v2")).unwrap();
    let meta2 = storage.meta_store.read_version("key").unwrap();
    assert_eq!(meta2.version, v2);
    let time2 = meta2.last_modified.clone();

    // times should be different (v2 is newer)
    assert!(
        time2 > time1,
        "newer version should have a later last_modified: v1={time1} v2={time2}"
    );
}

#[test]
fn test_last_modified_updated_on_metadata_change() {
    let tmp = support::test_dir("test_last_modified_metadata_update");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    rt().block_on(storage.put("key", b"data")).unwrap();
    let meta1 = storage.meta_store.read_version("key").unwrap();
    let time1 = meta1.last_modified.clone();

    // Small delay
    std::thread::sleep(Duration::from_millis(50));

    // Update metadata (Content-Type)
    storage.set_content_type("key", "text/plain").unwrap();

    let meta2 = storage.meta_store.read_version("key").unwrap();
    let time2 = meta2.last_modified.clone();

    // last_modified should have been updated
    assert!(
        time2 > time1,
        "last_modified should be updated on metadata change: t1={time1} t2={time2}"
    );

    // Content-Type should be set
    assert_eq!(
        meta2.metadata.get("content-type").map(|s| s.as_str()),
        Some("text/plain")
    );
}

#[test]
fn test_last_modified_in_list_response() {
    let tmp = support::test_dir("test_last_modified_in_list");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let keys = vec!["obj-x-a", "obj-x-b"];
    for key in &keys {
        rt().block_on(storage.put(key, b"data")).unwrap();
    }

    let entries = storage.list("", None, 100).unwrap();
    assert_eq!(entries.len(), 2);

    // Verify each object has last_modified set
    for key in &keys {
        let meta = storage.meta_store.read_version(key).unwrap();
        assert!(
            !meta.last_modified.is_empty(),
            "last_modified set for {key}"
        );
        assert!(
            chrono::DateTime::parse_from_rfc3339(&meta.last_modified).is_ok(),
            "last_modified is valid RFC 3339 for {key}"
        );
    }
}

#[test]
fn test_last_modified_version_metadata_stored_in_hashmap() {
    let tmp = support::test_dir("test_last_modified_in_hashmap");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    rt().block_on(storage.put("key", b"data")).unwrap();

    let meta = storage.meta_store.read_version("key").unwrap();

    // The last_modified should also be stored in the metadata hashmap
    // under the "last-modified" key
    let hashmap_time = meta
        .metadata
        .get("last-modified")
        .expect("last-modified should be in metadata hashmap");
    assert_eq!(hashmap_time, &meta.last_modified);

    // Both should be valid RFC 3339
    let _ = chrono::DateTime::parse_from_rfc3339(hashmap_time)
        .expect("metadata hashmap last-modified should be valid RFC 3339");
}

#[test]
fn test_last_modified_status_is_committed() {
    let tmp = support::test_dir("test_last_modified_status");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    rt().block_on(storage.put("key", b"data")).unwrap();

    let meta = storage.meta_store.read_version("key").unwrap();

    // Status should be Committed
    assert_eq!(meta.status, VersionStatus::Committed);

    // last_modified should be set
    assert!(!meta.last_modified.is_empty());
}
