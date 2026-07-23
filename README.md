# yotepack

**yotepack** is an erasure-coded, S3-style object storage server built in Rust. It stores objects reliably across multiple physical disks, tolerating disk failures transparently using Reed-Solomon erasure coding.

## Features

- **Unlimited object sizes** — objects are transparently split into fixed-size chunks (default 64 MiB)
- **Erasure coding** — Reed-Solomon encoding tolerates up to *M* simultaneous disk failures
- **Replicated metadata** — every metadata disk holds a full copy; one healthy disk is enough to read
- **MVCC versioning** — each object can have multiple versions; a version token lets you read your own write
- **Bitrot detection & auto-recovery** — per-shard checksums detect corruption and reconstruct damaged shards on read
- **Streaming writes** — supports objects larger than available RAM

## Quick Start

### Start the server

```bash
yotepack --disk /mnt/disk0 --disk /mnt/disk1 --disk /mnt/disk2 --failures 1
```

Each path points to the root of a physical disk (or partition). Each disk contains:

```
/mnt/disk0/
  metadata/    ← replicated metadata KV (one per disk, total = 2×failures+1)
  shards/      ← erasure-coded data shards

/mnt/disk1/
  metadata/
  shards/

/mnt/disk2/
  metadata/
  shards/
```

Each physical disk should ideally be on a separate drive. Specify one `--disk` path per shard slot using the `--failures` flag to control the number of shards:

| `--failures` | Disks needed | Space overhead |
|---|---|---|
| 1 | 3 | 2.00× |
| 2 | 5 | 1.67× |
| 3 | 7 | 1.50× |

### CLI options

```bash
yotepack --help

-d, --disk <PATH>...       Disk paths (one per shard, required). Total = failures×2+1
-f, --failures <N>         Disk failures to tolerate (default: 1)
    --chunk-size <BYTES>   Chunk size in bytes (default: 64MiB)
-p, --port <PORT>          Listen port (default: 8080, reserved for future HTTP API)
```

## What it does

yotepack is an object storage server. You store named objects (like an S3 bucket), and it handles:

- **Writing** — your data is split into chunks, erasure-coded across all configured disks, and metadata is replicated everywhere
- **Reading** — data is reconstructed from shards, with automatic correction of any corrupted bits
- **Versioning** — re-writing an object creates a new version; you can read any version by its token
- **Fault tolerance** — if disks fail, the system continues operating (as long as enough survive); metadata repair is available when disks come back online

## Crash recovery

On startup, yotepack automatically:

1. Promotes in-progress writes whose chunk data is fully on disk
2. Abandones in-progress writes with missing chunks
3. Runs garbage collection to reclaim space from deleted objects

## Error recovery

When a disk fails or comes back offline:

1. The system marks the disk as failed and continues using healthy disks
2. Once the disk is replaced or recovered, call `recover_meta_disk(idx, source)` to sync the full keyspace from a healthy disk
3. Data-layer recovery happens automatically on reads via Reed-Solomon reconstruction
