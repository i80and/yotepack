# yotepack

**yotepack** is an erasure-coded, S3-style object storage server built in Rust. It stores objects reliably across multiple physical disks, tolerating disk failures transparently using Reed-Solomon erasure coding.

## Features

- **Unlimited object sizes** — objects are transparently split into 64 MiB chunks
- **Erasure coding** — Reed-Solomon encoding tolerates up to *M* simultaneous disk failures (configurable at cluster creation)
- **Transparent compression** — optional zstd compression enabled via `compression_level` config; chunks are compressed before erasure coding
- **Replicated metadata** — every metadata disk holds a full copy; one healthy disk is enough to read
- **MVCC versioning** — each object can have multiple versions; a version token lets you read your own write
- **Bitrot detection & auto-recovery** — per-entry checksums detect corruption and automatically repair damaged shards on read
- **Byte range requests** — partial object reads via `Range` header (HTTP 206 Partial Content)
- **Streaming writes** — supports objects larger than available RAM

## Quick Start

### Start the server

```bash
yotepack --disk /mnt/disk0 --disk /mnt/disk1 --disk /mnt/disk2 --failures 1
```

Each path points to the root of a physical disk (or partition). Each disk contains:

```
/mnt/disk0/
  metadata/     ← replicated metadata KV (one per disk, total = 2×failures+1)
  segments/     ← erasure-coded mega-files (one per version per disk)
  wip/          ← in-progress writes (cleaned on startup)

/mnt/disk1/
  metadata/
  segments/
  wip/

/mnt/disk2/
  metadata/
  segments/
  wip/
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
-p, --port <PORT>          Listen port (default: 8080)
```

### Compression

Compression is configured at the disk path level via the `compression_level` config field.
Set it to any valid zstd level (1–22, higher = better compression, slower) or `None` for no compression.

When enabled, each chunk is compressed with zstd *before* erasure coding. The compression ratio
is stored in metadata alongside the uncompressed data size, allowing O(1) seeking:
- Offset for chunk N = `header_size + Σ(entry_size for chunks 0..N-1)`
- Each entry size = `24 + compressed_shard_size`
- Compressed shard size = `compressed_chunk_size / N`

Objects are transparently decompressed on read, so clients never see the compression.

## What it does

yotepack is an object storage server. You store named objects (like an S3 bucket), and it handles:

- **Writing** — your data is optionally compressed (zstd), split into 64 MiB chunks, erasure-coded across all configured disks, and metadata is replicated everywhere
- **Reading** — data is reconstructed from shards, with automatic correction of any corrupted bits. Byte range reads fetch only the overlapping chunks
- **Versioning** — re-writing an object creates a new version; you can read any version by its token
- **Fault tolerance** — if disks fail, the system continues operating (as long as enough survive); metadata repair is available when disks come back online

## Disk layout

Objects are stored as **mega-files**: each disk holds a single file per chunk index, containing all N shards packed back-to-back. This eliminates per-chunk file overhead. The last chunk may be smaller than 64 MiB (variable-length entries).

```
/mnt/disk0/segments/<safe_key>/
  v00000001    ← version 1 mega-file: [chunk0_shards][chunk1_shards][...]
  v00000002    ← version 2 mega-file: [chunk0_shards][chunk1_shards]
  v00000003    ← version 3 mega-file: [chunk0_shards]
```

Each version mega-file starts with a 4-byte header:
- Bytes 0–3: `object_format` (0 = uncompressed, 1 = zstd compressed) + 3 bytes padding

Each chunk is followed by N entries (one per disk/shard). Each entry has a 24-byte header:
- Bytes 0–15: per-shard checksum (u128)
- Bytes 16–23: shard data length (u64)
- Bytes 24+: shard data

The segment size is derived from the chunk's actual data length (not padded to 64 MiB), so small objects stay small on disk.

## Crash recovery

On startup, yotepack automatically:

1. Promotes in-progress writes whose chunk data is fully on disk
2. Abandons in-progress writes with missing chunks
3. Runs garbage collection to reclaim space from deleted objects

## Error recovery

When a disk fails or comes back offline:

1. The system marks the disk as failed and continues using healthy disks
2. Once the disk is replaced or recovered, call `recover_meta_disk(idx, source)` to sync the full keyspace from a healthy disk
3. Data-layer recovery happens automatically on reads via Reed-Solomon reconstruction
4. Bitrot detection identifies corrupted shard entries (via per-entry checksum verification); damaged shards are reconstructed from parity and the mega-file entry is rewritten atomically

## Erasure coding

- **Data shards (K)** = `failures + 1`
- **Parity shards (C)** = `failures`
- **Total shards (N)** = `2 × failures + 1`
- Uses GF(2^8) Reed-Solomon encoding (Reed-Solomon library)
- Tolerates up to *M* disk failures without data loss

## Byte range requests

The S3-compatible API supports HTTP `Range` headers:

```bash
curl -H "Range: bytes=0-1023" http://localhost:8080/mybucket/myfile
```

Returns HTTP 206 Partial Content with `Content-Range`, `Accept-Ranges: bytes`, and `Content-Length` headers. Only the chunks overlapping the requested range are read from disk.
