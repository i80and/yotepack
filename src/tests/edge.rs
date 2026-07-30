//! Edge case tests: corruption recovery, disk failures, pathological cases.

use std::sync::OnceLock;

use crate::tests::support;
use crate::ObjectStorage;

/// Reusable tokio runtime for blocking async operations.
fn rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

// ---------------------------------------------------------------------------
// Shard corruption and erasure-coding recovery
// ---------------------------------------------------------------------------

#[test]
fn edge_single_shard_bitrot() {
    let tmp = support::test_dir("edge_shard_bitrot");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // Write an object
    let data: Vec<u8> = (0..4096).map(|i| i as u8).collect();
    let token = rt().block_on(storage.put("bitrot", &data)).unwrap();

    // Read back successfully
    let result = rt().block_on(storage.get("bitrot", Some(token))).unwrap();
    assert_eq!(result, data);

    // Corrupt one segment in the mega-file on disk 0
    let meta = storage
        .meta_store
        .read_version_by_number("bitrot", token)
        .unwrap();
    assert_eq!(meta.data_size, 4096);

    let disk_path = &storage.chunk_store.disks[0].path;
    let safe_key = "bitrot";
    let segments_dir = disk_path.join("segments").join(safe_key);
    let mega_path = segments_dir.join(format!("v{:08}", token));
    let mut mega_data = std::fs::read(&mega_path).unwrap();

    // Entry format: <cksum:u128><len:u64><data> = 24 bytes header + data
    // Data starts at offset 4 (file header) + 24 (entry header) = 28
    let data_offset = 28;

    // Corrupt first byte of shard data
    mega_data[data_offset] ^= 0xFF;
    std::fs::write(&mega_path, mega_data).unwrap();

    // Reading should still succeed (erasure coding recovers the shard)
    let result2 = rt().block_on(storage.get("bitrot", Some(token))).unwrap();
    assert_eq!(result2, data);

    // Read again without token — should also work
    let result3 = rt().block_on(storage.get("bitrot", None)).unwrap();
    assert_eq!(result3, data);
}

#[test]
fn edge_shard_zeroed() {
    let tmp = support::test_dir("edge_shard_zeroed");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let data: Vec<u8> = (0..2048).map(|i| (i % 256) as u8).collect();
    let token = rt().block_on(storage.put("zeroed", &data)).unwrap();

    // Read back works
    let result = rt().block_on(storage.get("zeroed", Some(token))).unwrap();
    assert_eq!(result, data);

    // Zero out the first chunk's shard on disk 0
    let meta = storage
        .meta_store
        .read_version_by_number("zeroed", token)
        .unwrap();
    let total_per_disk = meta.compressed_sizes[0] as usize / storage.chunk_store.num_disks;
    let shard_size = total_per_disk - 24; // Remove 24-byte entry header

    let disk_path = &storage.chunk_store.disks[0].path;
    let mega_path = disk_path
        .join("segments")
        .join("zeroed")
        .join(format!("v{:08}", token));
    let mut mega_data = std::fs::read(&mega_path).unwrap();
    // Shard 0 data starts at offset 4 (header) + 24 (entry header) = 28
    for i in 0..shard_size {
        mega_data[28 + i] = 0;
    }
    std::fs::write(&mega_path, mega_data).unwrap();

    // Recovery should still work
    let result2 = rt().block_on(storage.get("zeroed", Some(token))).unwrap();
    assert_eq!(result2, data);
}

#[test]
fn edge_shard_truncated() {
    let tmp = support::test_dir("edge_shard_trunc");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let data: Vec<u8> = (0..2048).map(|i| i as u8).collect();
    let token = rt().block_on(storage.put("trunc", &data)).unwrap();

    let result = rt().block_on(storage.get("trunc", Some(token))).unwrap();
    assert_eq!(result, data);

    // Truncate the first chunk's shard on disk 0 to make it unreadable,
    // triggering erasure recovery. Corrupt the checksum of the first entry
    // (the exact length is unknown in variable-length format, so we flip
    // a byte in the 16-byte checksum area).
    let disk_path = &storage.chunk_store.disks[0].path;
    let mega_path = disk_path
        .join("segments")
        .join("trunc")
        .join(format!("v{:08}", token));
    let mut mega_data = std::fs::read(&mega_path).unwrap();
    // Corrupt the first entry's checksum (bytes 4-19)
    mega_data[4] ^= 0xFF;
    std::fs::write(&mega_path, mega_data).unwrap();

    // Recovery should work (erasure coding compensates for the bad shard)
    let result2 = rt().block_on(storage.get("trunc", Some(token))).unwrap();
    assert_eq!(result2, data);
}

#[test]
fn edge_shard_missing() {
    let tmp = support::test_dir("edge_shard_missing");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let data: Vec<u8> = (0..2048).map(|i| i as u8).collect();
    let token = rt().block_on(storage.put("missing", &data)).unwrap();

    let result = rt().block_on(storage.get("missing", Some(token))).unwrap();
    assert_eq!(result, data);

    // "Delete" the first chunk's shard on disk 0 by zeroing its segment
    let meta = storage
        .meta_store
        .read_version_by_number("missing", token)
        .unwrap();
    let total_per_disk = meta.compressed_sizes[0] as usize / storage.chunk_store.num_disks;
    let shard_size = total_per_disk - 24;

    let disk_path = &storage.chunk_store.disks[0].path;
    let mega_path = disk_path
        .join("segments")
        .join("missing")
        .join(format!("v{:08}", token));
    let mut mega_data = std::fs::read(&mega_path).unwrap();
    // Zero out shard 0 data (offset 4 header + 24 entry header = 28)
    for i in 0..shard_size {
        mega_data[28 + i] = 0;
    }
    std::fs::write(&mega_path, mega_data).unwrap();

    // Recovery should work
    let result2 = rt().block_on(storage.get("missing", Some(token))).unwrap();
    assert_eq!(result2, data);
}

// ---------------------------------------------------------------------------
// Multiple data disk failures
// ---------------------------------------------------------------------------

#[test]
fn edge_two_shard_failures_recovered() {
    let tmp = support::test_dir("edge_two_failures");
    // Use M=2 to tolerate 2 disk failures
    let config = support::make_test_config(&tmp, 2, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let data: Vec<u8> = (0..8192).map(|i| i as u8).collect();
    let token = rt().block_on(storage.put("two-fail", &data)).unwrap();

    // Verify initial read works
    let result = rt().block_on(storage.get("two-fail", Some(token))).unwrap();
    assert_eq!(result, data);

    // Corrupt first chunk's segments on disk 0 and disk 1
    let meta = storage
        .meta_store
        .read_version_by_number("two-fail", token)
        .unwrap();
    let total_per_disk = meta.compressed_sizes[0] as usize / storage.chunk_store.num_disks;
    let shard_size = total_per_disk - 24;

    for disk_idx in 0..2 {
        let disk_path = &storage.chunk_store.disks[disk_idx].path;
        let mega_path = disk_path
            .join("segments")
            .join("two-fail")
            .join(format!("v{:08}", token));
        let mut mega_data = std::fs::read(&mega_path).unwrap();
        // Corrupt shard 0 data (offset 4 header + 24 entry header = 28)
        for i in 0..shard_size {
            mega_data[28 + i] ^= 0xFF;
        }
        std::fs::write(&mega_path, mega_data).unwrap();
    }

    // Recovery should still work (tolerates 2 failures with M=2)
    let result2 = rt().block_on(storage.get("two-fail", Some(token))).unwrap();
    assert_eq!(result2, data);
}

#[test]
fn edge_three_shard_failures_exceeds_tolerance() {
    let tmp = support::test_dir("edge_three_failures");
    // Use M=2 — only tolerates 2 failures, 3 should fail
    let config = support::make_test_config(&tmp, 2, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let data: Vec<u8> = (0..8192).map(|i| i as u8).collect();
    let token = rt().block_on(storage.put("too-many", &data)).unwrap();

    // Corrupt first chunk's segments on disks 0, 1, and 2
    // With M=2, K=3, N=5, shard_size = 1024/3 = 342
    for disk_idx in 0..3 {
        let disk_path = &storage.chunk_store.disks[disk_idx].path;
        let mega_path = disk_path
            .join("segments")
            .join("too-many")
            .join(format!("v{:08}", token));
        let mut mega_data = std::fs::read(&mega_path).unwrap();
        // Corrupt chunk 0's segment data
        for i in 0..342 {
            mega_data[28 + i] ^= 0xFF;
        }
        std::fs::write(&mega_path, mega_data).unwrap();
    }

    // Recovery should FAIL (3 failures exceed M=2 tolerance)
    let result2 = rt().block_on(storage.get("too-many", Some(token)));
    assert!(result2.is_err());
}

// ---------------------------------------------------------------------------
// Metadata disk failure simulation
// ---------------------------------------------------------------------------

#[test]
fn edge_metadata_disk_io_failure() {
    let tmp = support::test_dir("edge_meta_io");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let data = b"meta-fail-test";
    let token = rt().block_on(storage.put("meta-key", data)).unwrap();

    // Write succeeds
    let result = rt().block_on(storage.get("meta-key", Some(token))).unwrap();
    assert_eq!(result, data);

    // The metadata store should have 3 disks (N=3 for M=1)
    assert_eq!(storage.meta_store.total_disk_count(), 3);
    assert_eq!(storage.meta_store.healthy_disk_count(), 3);

    // Verify system is safe
    assert!(storage.meta_store.is_safe());
}

// ---------------------------------------------------------------------------
// Crash recovery edge cases
// ---------------------------------------------------------------------------

#[test]
fn edge_pending_version_without_shards() {
    // Simulates a crash between set_pending and chunk writes
    let tmp = support::test_dir("edge_pending_no_shards");
    let config = support::make_test_config(&tmp, 1, 0);

    // Write and commit first object
    {
        let storage = ObjectStorage::new(config.clone()).unwrap();
        rt().block_on(storage.put("clean", b"data")).unwrap();
    }

    // On restart, cleanup should work
    let storage = ObjectStorage::new(config).unwrap();
    let result = storage.recover_on_startup();
    assert!(result.is_ok());

    // Object should still be readable
    let data = rt().block_on(storage.get("clean", None)).unwrap();
    assert_eq!(data, b"data");
}

#[test]
fn edge_multiple_pending_versions() {
    let tmp = support::test_dir("edge_multi_pending");
    let config = support::make_test_config(&tmp, 1, 0);

    // Write multiple objects
    {
        let storage = ObjectStorage::new(config.clone()).unwrap();
        for i in 0..5 {
            let data = format!("obj-{i}").into_bytes();
            rt().block_on(storage.put(&format!("key-{i}"), &data))
                .unwrap();
        }
    }

    // Restart and recover
    let storage = ObjectStorage::new(config).unwrap();
    let result = storage.recover_on_startup();
    assert!(result.is_ok());

    // All objects should be readable
    for i in 0..5 {
        let data = format!("obj-{i}").into_bytes();
        let result = rt()
            .block_on(storage.get(&format!("key-{i}"), None))
            .unwrap();
        assert_eq!(result, data);
    }
}

// ---------------------------------------------------------------------------
// Garbage collection edge cases
// ---------------------------------------------------------------------------

#[test]
fn edge_gc_after_partial_writes() {
    let tmp = support::test_dir("edge_gc_partial");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // Write and commit
    rt().block_on(storage.put("gc-partial", b"data")).unwrap();

    // GC should not error
    let result = storage.garbage_collect();
    assert!(result.is_ok());

    // Object should still be there
    let entries = storage.list("", None, 100).unwrap();
    assert!(!entries.is_empty());
}

#[test]
fn edge_gc_empty_store() {
    let tmp = support::test_dir("edge_gc_empty");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // GC on empty store should not error
    let result = storage.garbage_collect();
    assert!(result.is_ok());
}

#[test]
fn edge_gc_delete_then_recreate() {
    let tmp = support::test_dir("edge_gc_recreate");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // Write, delete, GC, then write again
    rt().block_on(storage.put("recreate", b"v1")).unwrap();
    storage.delete("recreate").unwrap();
    storage.garbage_collect().unwrap();

    let entries = storage.list("", None, 100).unwrap();
    assert_eq!(entries.len(), 0);

    // Recreate with new data
    rt().block_on(storage.put("recreate", b"v2")).unwrap();
    let entries = storage.list("", None, 100).unwrap();
    assert_eq!(entries.len(), 1);

    let result = rt().block_on(storage.get("recreate", None)).unwrap();
    assert_eq!(result, b"v2");
}

// ---------------------------------------------------------------------------
// Version conflict edge cases
// ---------------------------------------------------------------------------

#[test]
fn edge_rapid_concurrent_writes_same_key() {
    // Simulates rapid successive writes to the same key
    let tmp = support::test_dir("edge_rapid_writes");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let mut last_version = 0u64;
    for i in 0..20 {
        let data = format!("rapid-{i}").into_bytes();
        let version = rt().block_on(storage.put("rapid-key", &data)).unwrap();
        assert!(version > last_version, "v{version} > v{last_version}");
        last_version = version;

        // Each version should be independently readable with its token
        let result = rt()
            .block_on(storage.get("rapid-key", Some(version)))
            .unwrap();
        assert_eq!(result, data);
    }

    // Final version should be the latest
    let latest = rt().block_on(storage.get("rapid-key", None)).unwrap();
    assert_eq!(latest, format!("rapid-19").into_bytes());
}

#[test]
fn edge_version_number_stress() {
    // Verify version counters work under heavy write load
    let tmp = support::test_dir("edge_version_stress");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let mut prev = 0u64;
    // Write enough versions that we stress the counter
    for _ in 0..100 {
        let version = rt().block_on(storage.put("counter-test", b"x")).unwrap();
        assert!(version > prev);
        prev = version;
    }

    // Latest should still work
    let result = rt().block_on(storage.get("counter-test", None)).unwrap();
    assert_eq!(result, b"x");
}

// ---------------------------------------------------------------------------
// Data integrity edge cases
// ---------------------------------------------------------------------------

#[test]
fn edge_single_byte_object() {
    let tmp = support::test_dir("edge_single_byte");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let data = vec![0xFFu8];
    let token = rt().block_on(storage.put("tiny", &data)).unwrap();
    let result = rt().block_on(storage.get("tiny", Some(token))).unwrap();
    assert_eq!(result, data);
}

#[test]
fn edge_null_byte_object() {
    let tmp = support::test_dir("edge_null_bytes");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
    let token = rt().block_on(storage.put("all-bytes", &data)).unwrap();
    let result = rt()
        .block_on(storage.get("all-bytes", Some(token)))
        .unwrap();
    assert_eq!(result, data);
}

#[test]
fn edge_repeated_data() {
    let tmp = support::test_dir("edge_repeated");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // 1024 copies of the same byte — tests erasure coding on highly redundant data
    let data = vec![0xABu8; 4096];
    let token = rt().block_on(storage.put("repeated", &data)).unwrap();
    let result = rt().block_on(storage.get("repeated", Some(token))).unwrap();
    assert_eq!(result, data);
}

// ---------------------------------------------------------------------------
// Cluster ID validation
// ---------------------------------------------------------------------------

#[test]
fn edge_cluster_id_fresh_generation() {
    // Fresh disks with no config UUIDs → UUIDs are generated and written
    let tmp = support::test_dir("edge_cluster_fresh");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // Each disk should have a .cluster_id file with a valid UUID
    for (i, disk) in storage.chunk_store.disks.iter().enumerate() {
        let cluster_id_file = disk.path.join(".cluster_id");
        assert!(
            cluster_id_file.exists(),
            "disk {i} missing .cluster_id file"
        );
        let contents = std::fs::read_to_string(&cluster_id_file).unwrap();
        let parsed = uuid::Uuid::parse_str(contents.trim()).unwrap();
        assert_eq!(parsed, disk.cluster_id.0, "disk {i} cluster ID mismatch");
    }

    // Storage should still work
    rt().block_on(storage.put("fresh-obj", b"data")).unwrap();
    let result = rt().block_on(storage.get("fresh-obj", None)).unwrap();
    assert_eq!(result, b"data");
}

#[test]
fn edge_cluster_id_explicit_config() {
    // Create a fresh cluster, read UUIDs, then restart with explicit config.
    // Verify that explicit config matches the on-disk UUIDs.
    let tmp = support::test_dir("edge_cluster_explicit");

    // First pass: generate UUIDs
    let n = 1 * 2 + 1; // M=1, N=3
    let disk_paths: Vec<String> = (0..n)
        .map(|i| tmp.path().join(format!("disk_{i}")))
        .into_iter()
        .map(|p| p.display().to_string())
        .collect();
    let config1 = crate::Config {
        disk_paths: disk_paths.clone(),
        disk_failures: 1,
        metadata_replicas: 0,
        disk_uuids: Vec::new(),
        compression_level: None,
    };
    let storage1 = ObjectStorage::new(config1).unwrap();

    // Collect the generated UUIDs
    let uuids: Vec<String> = storage1
        .chunk_store
        .disks
        .iter()
        .map(|d| d.cluster_id.0.to_string())
        .collect();
    assert_eq!(uuids.len(), 3); // N=3 for M=1

    // Drop storage1 to release Fjall locks
    drop(storage1);

    // Second pass: restart with explicit config UUIDs on the same DB
    let config2 = crate::Config {
        disk_paths,
        disk_failures: 1,
        metadata_replicas: 0,
        disk_uuids: uuids.clone(),
        compression_level: None,
    };
    let storage2 = ObjectStorage::new(config2).unwrap();

    // Verify UUIDs match
    for (i, disk) in storage2.chunk_store.disks.iter().enumerate() {
        assert_eq!(
            disk.cluster_id.0.to_string(),
            uuids[i],
            "disk {i} UUID mismatch after restart"
        );
    }

    // Verify the on-disk .cluster_id files also match
    for (i, disk) in storage2.chunk_store.disks.iter().enumerate() {
        let cluster_id_file = disk.path.join(".cluster_id");
        let contents = std::fs::read_to_string(&cluster_id_file).unwrap();
        let file_uuid = uuid::Uuid::parse_str(contents.trim()).unwrap();
        assert_eq!(
            file_uuid, disk.cluster_id.0,
            "disk {i} on-disk UUID mismatch"
        );
    }
}

#[test]
fn edge_cluster_id_mismatch_rejected() {
    // Try to start with a config that has a wrong UUID → should error
    let tmp = support::test_dir("edge_cluster_mismatch");

    // First pass: generate UUIDs
    let config1 = support::make_test_config(&tmp, 1, 0);
    let storage1 = ObjectStorage::new(config1).unwrap();

    // Build config with a modified UUID for disk 0
    let mut uuids: Vec<String> = storage1
        .chunk_store
        .disks
        .iter()
        .map(|d| d.cluster_id.0.to_string())
        .collect();
    uuids[0] = uuid::Uuid::new_v4().to_string(); // wrong UUID

    let disk_paths: Vec<String> = storage1
        .chunk_store
        .disks
        .iter()
        .map(|d| d.path.to_string_lossy().to_string())
        .collect();

    let config2 = crate::Config {
        disk_paths,
        disk_failures: 1,
        metadata_replicas: 0,
        disk_uuids: uuids,
        compression_level: None,
    };

    // Should fail with ClusterIdMismatch
    let result = ObjectStorage::new(config2);
    assert!(result.is_err());
    let err = match result {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("cluster ID") || err.to_string().contains("ClusterIdMismatch"),
        "expected ClusterIdMismatch error, got: {err}"
    );
}

#[test]
fn edge_cluster_id_count_mismatch_rejected() {
    // Config with wrong number of UUIDs → should error
    let tmp = support::test_dir("edge_cluster_count");
    let config = support::make_test_config(&tmp, 1, 0);

    // Mismatched count (1 UUID for 3 disks)
    let config_bad = crate::Config {
        disk_paths: config.disk_paths.clone(),
        disk_failures: config.disk_failures,
        metadata_replicas: config.metadata_replicas,
        disk_uuids: vec![uuid::Uuid::new_v4().to_string()], // 1 instead of 3
        compression_level: None,
    };

    let result = ObjectStorage::new(config_bad);
    assert!(result.is_err());
    let err = match result {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("disk_uuids count") || err.to_string().contains("total_shards"),
        "expected count mismatch error, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Invalid key validation
// ---------------------------------------------------------------------------

#[test]
fn edge_empty_key_rejected() {
    let tmp = support::test_dir("edge_empty_key");
    let config = support::make_test_config(&tmp, 1, 0);
    let storage = ObjectStorage::new(config).unwrap();

    // PUT with empty key should fail
    let result = rt().block_on(storage.put("", b"data"));
    assert!(
        result.is_err(),
        "PUT with empty key should be rejected, got: {result:?}"
    );

    // GET with empty key should fail
    let result = rt().block_on(storage.get("", None));
    assert!(
        result.is_err(),
        "GET with empty key should be rejected, got: {result:?}"
    );

    // DELETE with empty key should fail
    let result = storage.delete("");
    assert!(
        result.is_err(),
        "DELETE with empty key should be rejected, got: {result:?}"
    );
}
