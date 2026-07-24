//! S3-compatible HTTP API server.
//!
//! This module provides the HTTP server layer for an S3-compatible object storage API.
//! It is built on top of `axum` and exposes the core `ObjectStorage` operations
//! as S3-style REST endpoints.
//!
//! # Planned Endpoints
//!
//! | S3 Operation      | HTTP Method | Path Pattern       | Handler           |
//! |-------------------|-------------|--------------------|-------------------|
//! | PutBucket         | PUT         | `/` (with `?bucket`) | `create_bucket`   |
//! | ListBuckets       | GET         | `/`               | `list_buckets`    |
//! | PutObject         | PUT         | `/:bucket/:key`    | `put_object`      |
//! | GetObject         | GET         | `/:bucket/:key`    | `get_object`      |
//! | DeleteObject      | DELETE      | `/:bucket/:key`    | `delete_object`   |
//! | HeadObject        | HEAD        | `/:bucket/:key`    | `head_object`     |
//! | ListObjects       | GET         | `/:bucket`         | `list_objects`    |
//! | GetBucketAcl      | GET         | `/:bucket`         | `get_bucket_acl`  |
//! | PutBucketAcl      | PUT         | `/:bucket`         | `put_bucket_acl`  |
//! | GetObjectAcl      | GET         | `/:bucket/:key`    | `get_object_acl`  |
//! | PutObjectAcl      | PUT         | `/:bucket/:key`    | `put_object_acl`  |
//! | GetBucketVersioning | GET       | `/:bucket`         | `get_bucket_versioning` |
//! | PutBucketVersioning | PUT       | `/:bucket`         | `put_bucket_versioning` |

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, head, put},
    Json, Router,
};
use chrono::Utc;
use md5::{Digest, Md5};
use std::sync::Arc;

// Parse bucket and key from a full path like "/bucket/subdir/file.txt"
#[allow(dead_code)]
fn parse_bucket_key(full_path: &str) -> Option<(String, String)> {
    let path = full_path.strip_prefix('/').unwrap_or(full_path);
    let mut parts = path.splitn(2, '/');
    let bucket = parts.next()?;
    let key = parts.next().unwrap_or("");
    if bucket.is_empty() {
        return None;
    }
    Some((bucket.to_string(), key.to_string()))
}

use crate::api::ObjectStorage;
use crate::disk::{VersionMeta, VersionStatus};
use crate::errors::StorageError;

// =============================================================================
// Request/Response Types (placeholders for future implementation)
// =============================================================================

/// Response structure for `ListBuckets`.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ListBucketsResponse {
    pub buckets: Vec<BucketInfo>,
}

/// Information about a single bucket.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct BucketInfo {
    pub name: String,
    pub creation_date: String,
}

/// Query parameters for `ListObjects`.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct ListObjectsQuery {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub marker: Option<String>,
    pub max_keys: Option<u32>,
}

/// Response structure for `ListObjects`.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ListObjectsResponse {
    pub name: String,
    pub prefix: Option<String>,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub max_keys: usize,
    pub is_truncated: bool,
    pub contents: Vec<ObjectEntry>,
    pub common_prefixes: Vec<CommonPrefix>,
}

/// A common prefix entry in a `ListObjects` response (for delimiter grouping).
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct CommonPrefix {
    pub prefix: String,
}

/// A single object entry in a `ListObjects` response.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ObjectEntry {
    pub key: String,
    pub last_modified: String,
    pub etag: String,
    pub size: usize,
    pub storage_class: String,
}

/// Bucket metadata (stored on disk, placeholder structure).
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct BucketMetadata {
    pub name: String,
    pub created_at: String,
}

/// Application state shared across request handlers.
#[derive(Clone)]
pub struct S3AppState {
    pub storage: Arc<ObjectStorage>,
}

// =============================================================================
// Router Setup
// =============================================================================

/// Build the S3 API router with all registered routes.
///
/// This function wires up the S3 endpoint handlers to the Axum router.
/// Individual handlers are stubs at this point and will be filled in future steps.
pub fn build_router(storage: Arc<ObjectStorage>) -> Router {
    let state = S3AppState { storage };

    Router::new()
        // Bucket operations (must come first for exact match)
        .route("/", put(create_bucket))
        .route("/", get(list_buckets))
        .route("/:bucket", get(list_objects))
        .route("/:bucket", delete(delete_bucket))
        .route("/:bucket", put(create_bucket))
        // Object operations (multi-segment, comes after bucket routes)
        .route("/:bucket/*key", put(put_object))
        .route("/:bucket/*key", get(get_object))
        .route("/:bucket/*key", delete(delete_object))
        .route("/:bucket/*key", head(head_object))
        .with_state(state)
}

// =============================================================================
// Error Handling
// =============================================================================

/// Convert `StorageError` into an Axum-compatible HTTP response.
fn storage_error_to_response(err: StorageError) -> (StatusCode, String) {
    match &err {
        StorageError::NotFound(key) => (StatusCode::NOT_FOUND, format!("Not Found: {key}")),
        StorageError::BucketAlreadyExists(name) => (
            StatusCode::CONFLICT,
            format!("Bucket already exists: {name}"),
        ),
        StorageError::BucketNotFound(name) => {
            (StatusCode::NOT_FOUND, format!("Bucket not found: {name}"))
        }
        StorageError::ChecksumMismatch { expected, actual } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Checksum mismatch: expected {expected}, got {actual}"),
        ),
        StorageError::VersionConflict => (
            StatusCode::CONFLICT,
            "Version conflict: concurrent write detected".to_string(),
        ),
        _ => {
            tracing::error!("Storage error: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            )
        }
    }
}

/// Convert a `StorageError` into an Axum-compatible response.
fn error_to_response(err: StorageError) -> Response {
    let (status, body) = storage_error_to_response(err);
    let mut res = Response::new(axum::body::Body::from(body));
    *res.status_mut() = status;
    res
}

// =============================================================================
// Bucket Handlers (stubs)
// =============================================================================

/// PUT /:bucket → CreateBucket
///
/// Create a new bucket with the given name.
async fn create_bucket(State(state): State<S3AppState>, Path(bucket): Path<String>) -> Response {
    // Validate bucket name
    if bucket.is_empty() || bucket.contains('/') || bucket.contains(':') {
        return error_to_response(StorageError::BucketNotFound(format!(
            "Invalid bucket name: {bucket}"
        )));
    }

    match state.storage.create_bucket(&bucket) {
        Ok(()) => {
            let mut res = Response::new(axum::body::Body::empty());
            *res.status_mut() = StatusCode::CREATED;
            res
        }
        Err(StorageError::BucketAlreadyExists(_)) => {
            (StatusCode::CONFLICT, "Bucket already exists").into_response()
        }
        Err(e) => error_to_response(e),
    }
}

/// GET / → ListBuckets
///
/// List all buckets owned by this storage instance.
async fn list_buckets(State(state): State<S3AppState>) -> impl IntoResponse {
    let buckets = match state.storage.list_buckets() {
        Ok(buckets) => buckets,
        Err(e) => return error_to_response(e),
    };

    let response = ListBucketsResponse {
        buckets: buckets
            .into_iter()
            .map(|b| BucketInfo {
                name: b.name,
                creation_date: b.created_at,
            })
            .collect(),
    };
    (StatusCode::OK, Json(response)).into_response()
}

/// DELETE /:bucket → DeleteBucket
///
/// Delete an empty bucket. Returns 409 if the bucket is not empty.
async fn delete_bucket(State(state): State<S3AppState>, Path(bucket): Path<String>) -> Response {
    // Validate bucket name
    if bucket.is_empty() || bucket.contains('/') || bucket.contains(':') {
        return error_to_response(StorageError::BucketNotFound(format!(
            "Invalid bucket name: {bucket}"
        )));
    }

    match state.storage.delete_bucket(&bucket) {
        Ok(()) => {
            let mut res = Response::new(axum::body::Body::empty());
            *res.status_mut() = StatusCode::NO_CONTENT;
            res
        }
        Err(StorageError::NotFound(_)) => (
            StatusCode::NOT_FOUND,
            format!("Bucket '{bucket}' not found"),
        )
            .into_response(),
        Err(StorageError::Transient(msg)) if msg.contains("not empty") => (
            StatusCode::CONFLICT,
            format!("Bucket '{bucket}' is not empty"),
        )
            .into_response(),
        Err(e) => error_to_response(e),
    }
}

// =============================================================================
// Object Handlers (stubs)
// =============================================================================

/// GET /:bucket → ListObjects
///
/// List objects in a bucket, optionally filtered by prefix/marker.
async fn list_objects(
    State(state): State<S3AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<ListObjectsQuery>,
) -> Response {
    // Validate bucket name
    if bucket.is_empty() || bucket.contains('/') || bucket.contains(':') {
        return error_to_response(StorageError::NotFound(format!(
            "Invalid bucket name: {bucket}"
        )));
    }

    let prefix = params.prefix.as_deref().unwrap_or("");
    let delimiter = params.delimiter.as_deref().unwrap_or("");
    let marker = params.marker.as_deref();
    let max_keys = params.max_keys.unwrap_or(1000) as usize;

    // Build storage prefix: "bucket/" or "bucket/prefix"
    let storage_prefix = if prefix.is_empty() {
        format!("{bucket}/")
    } else {
        format!("{bucket}/{prefix}")
    };

    // Fetch all versions (limit * 2 to account for version dedup)
    let entries = match state
        .storage
        .meta_store
        .scan_versions(&storage_prefix, max_keys * 2)
    {
        Ok(e) => e,
        Err(e) => return error_to_response(e),
    };

    // Group by latest version per object key, keeping track of the object key
    // Key in entries is "ver:bucket/key", we strip "ver:" to get "bucket/key"
    let mut latest: std::collections::HashMap<String, (String, VersionMeta)> =
        std::collections::HashMap::new();
    for (raw_key, meta) in entries {
        if meta.status != VersionStatus::Committed {
            continue;
        }
        let object_key = raw_key.strip_prefix("ver:").unwrap_or(&raw_key).to_string();
        let existing = latest.entry(object_key).or_insert_with(|| {
            let obj_key = raw_key.strip_prefix("ver:").unwrap_or(&raw_key).to_string();
            (obj_key, meta.clone())
        });
        if meta.version > existing.1.version {
            existing.1 = meta;
        }
    }

    // Collect as sorted vec of (object_key, meta)
    let mut sorted: Vec<_> = latest.into_values().collect();
    sorted.sort_by_key(|(_, m)| m.chunk_ids.first().cloned().unwrap_or_default());

    // Filter by marker (skip entries before the marker)
    if let Some(marker_str) = marker {
        sorted.retain(|(_, m)| {
            m.chunk_ids
                .first()
                .map(|cid| cid.as_str() > marker_str)
                .unwrap_or(false)
        });
    }

    // Determine truncation
    let is_truncated = sorted.len() > max_keys;
    let next_marker = if is_truncated {
        sorted
            .get(max_keys)
            .and_then(|(_, m)| m.chunk_ids.first().cloned())
    } else {
        None
    };

    // Take only the needed entries
    let sorted = if is_truncated {
        sorted.drain(..max_keys).collect()
    } else {
        sorted
    };

    // Build response
    let (contents, common_prefixes) = if delimiter.is_empty() {
        let contents: Vec<ObjectEntry> = sorted
            .into_iter()
            .map(|(key, meta)| ObjectEntry {
                key,
                last_modified: Utc::now().to_rfc3339(),
                etag: format!("{:x}", meta.checksum),
                size: meta.data_size,
                storage_class: "STANDARD".to_string(),
            })
            .collect();
        (contents, Vec::new())
    } else {
        // Group by common prefix using delimiter
        let mut groups: std::collections::BTreeMap<String, ()> = std::collections::BTreeMap::new();
        let mut contents: Vec<ObjectEntry> = Vec::new();

        for (key, meta) in sorted {
            // The key includes the storage prefix, e.g. "bucket/prefix/file.txt"
            // The suffix is everything after the storage prefix, e.g. "file.txt" or "dir/file.txt"
            let suffix = key.strip_prefix(&storage_prefix).unwrap_or(&key);

            if let Some(pos) = suffix.find(delimiter) {
                // There's a delimiter in the suffix — this contributes to common prefixes
                let common = format!("{prefix}{}", &suffix[..=pos]);
                groups.insert(common, ());
            } else {
                // No delimiter — this is a leaf object
                contents.push(ObjectEntry {
                    key,
                    last_modified: Utc::now().to_rfc3339(),
                    etag: format!("{:x}", meta.checksum),
                    size: meta.data_size,
                    storage_class: "STANDARD".to_string(),
                });
            }
        }

        let common_prefixes: Vec<CommonPrefix> = groups
            .into_keys()
            .map(|p| CommonPrefix { prefix: p })
            .collect();
        (contents, common_prefixes)
    };

    let response = ListObjectsResponse {
        name: bucket,
        prefix: params.prefix.clone(),
        marker: params.marker.clone(),
        next_marker,
        max_keys: params.max_keys.unwrap_or(1000) as usize,
        is_truncated,
        contents,
        common_prefixes,
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// PUT /:bucket/:key → PutObject
///
/// Upload an object to a bucket. Supports both single-shot and streaming uploads.
async fn put_object(
    state: State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
    req: axum::extract::Request,
) -> Response {
    // Validate bucket name
    if bucket.is_empty() || bucket.contains('/') || bucket.contains(':') {
        return error_to_response(StorageError::NotFound(format!(
            "Invalid bucket name: {bucket}"
        )));
    }

    // Validate key
    if key.is_empty() {
        return error_to_response(StorageError::NotFound(
            "Object key cannot be empty".to_string(),
        ));
    }

    // Extract body manually
    let body = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .unwrap();

    // Build full object key (bucket/key prefix for namespacing)
    let object_key = format!("{bucket}/{key}");

    // Store the object
    let token = match state.storage.put(&object_key, &body).await {
        Ok(t) => t,
        Err(e) => return error_to_response(e),
    };

    // Calculate ETag as MD5 of the body
    let mut hasher = Md5::new();
    hasher.update(&body);
    let etag = format!("{:x}", hasher.finalize());

    let mut headers = HeaderMap::new();
    headers.insert("x-amz-version-id", token.to_string().parse().unwrap());
    headers.insert("etag", etag.parse().unwrap());
    headers.insert("content-length", body.len().to_string().parse().unwrap());

    let mut res = Response::new(axum::body::Body::from(body));
    *res.headers_mut() = headers;
    res
}

/// GET /:bucket/:key → GetObject
///
/// Retrieve an object from a bucket.
async fn get_object(
    State(state): State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    // Validate bucket name
    if bucket.is_empty() || bucket.contains('/') || bucket.contains(':') {
        return error_to_response(StorageError::NotFound(format!(
            "Invalid bucket name: {bucket}"
        )));
    }

    // Validate key
    if key.is_empty() {
        return error_to_response(StorageError::NotFound(
            "Object key cannot be empty".to_string(),
        ));
    }

    // Build full object key
    let object_key = format!("{bucket}/{key}");

    // Retrieve the object
    let data = match state.storage.get(&object_key, None).await {
        Ok(d) => d,
        Err(e) => return error_to_response(e),
    };

    // Calculate ETag
    let mut hasher = Md5::new();
    hasher.update(&data);
    let etag = format!("{:x}", hasher.finalize());

    let mut headers = HeaderMap::new();
    headers.insert("content-length", data.len().to_string().parse().unwrap());
    headers.insert("etag", etag.parse().unwrap());

    let mut res = Response::new(axum::body::Body::from(data));
    *res.headers_mut() = headers;
    res
}

/// DELETE /:bucket/:key → DeleteObject
///
/// Delete an object from a bucket.
async fn delete_object(
    State(state): State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    // Validate bucket name
    if bucket.is_empty() || bucket.contains('/') || bucket.contains(':') {
        return error_to_response(StorageError::NotFound(format!(
            "Invalid bucket name: {bucket}"
        )));
    }

    // Validate key
    if key.is_empty() {
        return error_to_response(StorageError::NotFound(
            "Object key cannot be empty".to_string(),
        ));
    }

    // Build full object key
    let object_key = format!("{bucket}/{key}");

    // Delete the object
    match state.storage.delete(&object_key) {
        Ok(()) => {
            let mut res = Response::new(axum::body::Body::empty());
            *res.status_mut() = StatusCode::NO_CONTENT;
            res
        }
        Err(e) => error_to_response(e),
    }
}

/// HEAD /:bucket/:key → HeadObject
///
/// Retrieve metadata about an object without downloading its body.
async fn head_object(
    State(_state): State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    // TODO: Validate bucket exists
    // TODO: Call storage.get() but discard body, only return headers
    // TODO: Return Content-Length, ETag, Last-Modified headers
    (
        StatusCode::NOT_IMPLEMENTED,
        format!("HeadObject '{bucket}/{key}' not yet implemented"),
    )
}

// =============================================================================
// ACL Handlers (stubs)
// =============================================================================

/// GET /:bucket?acl → GetBucketAcl
#[allow(dead_code)]
async fn get_bucket_acl(
    State(_state): State<S3AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let _ = bucket;
    (
        StatusCode::NOT_IMPLEMENTED,
        "GetBucketAcl not yet implemented",
    )
}

/// PUT /:bucket?acl → PutBucketAcl
#[allow(dead_code)]
async fn put_bucket_acl(
    State(_state): State<S3AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let _ = bucket;
    (
        StatusCode::NOT_IMPLEMENTED,
        "PutBucketAcl not yet implemented",
    )
}

/// GET /:bucket/:key?acl → GetObjectAcl
#[allow(dead_code)]
async fn get_object_acl(
    State(_state): State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let _ = bucket;
    let _ = key;
    (
        StatusCode::NOT_IMPLEMENTED,
        "GetObjectAcl not yet implemented",
    )
}

/// PUT /:bucket/:key?acl → PutObjectAcl
#[allow(dead_code)]
async fn put_object_acl(
    State(_state): State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let _ = bucket;
    let _ = key;
    (
        StatusCode::NOT_IMPLEMENTED,
        "PutObjectAcl not yet implemented",
    )
}

// =============================================================================
// Versioning Handlers (stubs)
// =============================================================================

/// GET /:bucket?versioning → GetBucketVersioning
#[allow(dead_code)]
async fn get_bucket_versioning(
    State(_state): State<S3AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let _ = bucket;
    (
        StatusCode::NOT_IMPLEMENTED,
        "GetBucketVersioning not yet implemented",
    )
}

/// PUT /:bucket?versioning → PutBucketVersioning
#[allow(dead_code)]
async fn put_bucket_versioning(
    State(_state): State<S3AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let _ = bucket;
    (
        StatusCode::NOT_IMPLEMENTED,
        "PutBucketVersioning not yet implemented",
    )
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ObjectStorage;
    use crate::config::Config;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use axum::Router;
    use tempfile::TempDir;
    use tower::util::ServiceExt;

    fn make_test_config(tmp: &TempDir) -> Config {
        let n = 1 * 2 + 1; // M=1, N=3
        let disk_paths: Vec<String> = (0..n)
            .map(|i| tmp.path().join(format!("disk_{i}")))
            .into_iter()
            .map(|p| p.display().to_string())
            .collect();
        Config {
            disk_paths,
            disk_failures: 1,
            chunk_size: 1024,
            metadata_replicas: 0,
            disk_uuids: Vec::new(),
        }
    }

    fn make_test_router(tmp: TempDir) -> Router {
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let state = S3AppState {
            storage: Arc::new(storage),
        };
        Router::new()
            .route("/health", get(|| async { "OK" }))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_health_check() {
        let tmp = TempDir::new().unwrap();
        let app = make_test_router(tmp);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_put_object() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        let body = axum::body::Bytes::from(b"Hello, S3 world!".to_vec());
        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/mybucket/test.txt")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Check headers
        let headers = response.headers();
        assert!(headers.contains_key("etag"));
        assert!(headers.contains_key("content-length"));
        assert!(headers.contains_key("x-amz-version-id"));
        assert_eq!(headers.get("content-length").unwrap(), "16");
    }

    #[tokio::test]
    async fn test_get_object() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // First put the object
        let put_body = axum::body::Bytes::from(b"test data for retrieval".to_vec());
        let put_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/mybucket/retrieval.txt")
                    .body(Body::from(put_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put_response.status(), StatusCode::OK);

        // Now get the object
        let get_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/mybucket/retrieval.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(get_response.status(), StatusCode::OK);

        // Check headers
        let headers = get_response.headers();
        assert!(headers.contains_key("etag"));
        assert!(headers.contains_key("content-length"));
        assert_eq!(headers.get("content-length").unwrap(), "23");

        // Get body
        let body_bytes = axum::body::to_bytes(get_response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            body_bytes,
            axum::body::Bytes::from(b"test data for retrieval".as_slice())
        );
    }

    #[tokio::test]
    async fn test_get_nonexistent_object() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/mybucket/nonexistent.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_put_large_object() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // Create data larger than chunk size
        let data: Vec<u8> = (0..4096u16).map(|i| (i % 256) as u8).collect();
        let body = axum::body::Bytes::from(data.clone());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/mybucket/large.bin")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify we can retrieve it
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/mybucket/large.bin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body_bytes, data);
    }

    #[tokio::test]
    async fn test_delete_object() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // Put an object
        let body = axum::body::Bytes::from(b"to be deleted".to_vec());
        let _response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/mybucket/delete-test.txt")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Verify it exists
        let get_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/mybucket/delete-test.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);

        // Delete it
        let delete_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("http://localhost/mybucket/delete-test.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

        // Verify it's gone
        let get_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/mybucket/delete-test.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_list_objects_empty() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // List an empty bucket
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/emptybucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: ListObjectsResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(resp.name, "emptybucket");
        assert_eq!(resp.contents.len(), 0);
        assert_eq!(resp.is_truncated, false);
    }

    #[tokio::test]
    async fn test_list_objects_after_puts() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // Put several objects
        let bodies = vec![
            ("dir1/file1.txt", b"hello world".as_slice()),
            ("dir1/file2.txt", b"foo bar baz".as_slice()),
            ("dir2/file3.txt", b"qux".as_slice()),
            ("root.txt", b"root content".as_slice()),
        ];

        for (key, data) in &bodies {
            let body = axum::body::Bytes::from(data.to_vec());
            let _response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("http://localhost/mybucket/{key}"))
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            // We don't assert on PUT here since the bucket must exist
            // but the handler returns NOT_IMPLEMENTED for create_bucket
        }

        // List all objects
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/mybucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: ListObjectsResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(resp.name, "mybucket");
        // Should list all 4 objects
        assert_eq!(resp.contents.len(), 4);
        assert_eq!(resp.is_truncated, false);
    }

    #[tokio::test]
    async fn test_list_objects_with_prefix() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // Put several objects with different prefixes
        let bodies = vec![
            ("alpha/a1.txt", b"a1".as_slice()),
            ("alpha/a2.txt", b"a2".as_slice()),
            ("beta/b1.txt", b"b1".as_slice()),
            ("beta/b2.txt", b"b2".as_slice()),
        ];

        for (key, data) in &bodies {
            let body = axum::body::Bytes::from(data.to_vec());
            let _response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("http://localhost/testbucket/{key}"))
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // List with prefix "alpha/"
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/testbucket?prefix=alpha/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: ListObjectsResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(resp.name, "testbucket");
        assert_eq!(resp.contents.len(), 2);
        assert!(resp.contents.iter().all(|o| o.key.contains("alpha/")));
    }

    // =====================================================================
    // Bucket tests
    // =====================================================================

    #[tokio::test]
    async fn test_create_bucket() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // Create a bucket
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/newbucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_create_bucket_already_exists() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // Create the bucket first time
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/mybucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        // Try to create it again
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/mybucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_list_buckets_empty() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: ListBucketsResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(resp.buckets.len(), 0);
    }

    #[tokio::test]
    async fn test_list_buckets_with_multiple() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // Create several buckets
        for name in &["alpha", "beta", "gamma"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("http://localhost/{name}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        // List all buckets
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: ListBucketsResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(resp.buckets.len(), 3);

        // Verify they're sorted
        assert_eq!(resp.buckets[0].name, "alpha");
        assert_eq!(resp.buckets[1].name, "beta");
        assert_eq!(resp.buckets[2].name, "gamma");

        // Verify each bucket has a creation date
        for bucket in &resp.buckets {
            assert!(!bucket.creation_date.is_empty());
        }
    }

    #[tokio::test]
    async fn test_delete_bucket_empty() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // Create and delete an empty bucket
        let _response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/empty-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("http://localhost/empty-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Verify it's gone
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: ListBucketsResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(resp.buckets.len(), 0);
    }

    #[tokio::test]
    async fn test_delete_bucket_not_empty() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // Create bucket and add an object
        let _response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/mybucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::Bytes::from(b"hello".to_vec());
        let _response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/mybucket/file.txt")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Try to delete — should fail because bucket is not empty
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("http://localhost/mybucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_delete_bucket_not_found() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("http://localhost/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_bucket_isolation() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // Create two buckets
        for name in &["bucket-a", "bucket-b"] {
            let _response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("http://localhost/{name}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Put same key in both buckets
        let body = axum::body::Bytes::from(b"hello".to_vec());
        let _response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/bucket-a/same.txt")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::Bytes::from(b"world".to_vec());
        let _response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/bucket-b/same.txt")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Verify each bucket only sees its own objects
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/bucket-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: ListObjectsResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(resp.contents.len(), 1);
        assert!(resp.contents[0].key.contains("bucket-a"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/bucket-b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let resp: ListObjectsResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(resp.contents.len(), 1);
        assert!(resp.contents[0].key.contains("bucket-b"));

        // Verify correct data retrieval
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/bucket-a/same.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body_bytes, axum::body::Bytes::from(&b"hello"[..]));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/bucket-b/same.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body_bytes, axum::body::Bytes::from(&b"world"[..]));
    }
}
