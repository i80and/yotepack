/// S3-compatible HTTP API server.
///
/// This module provides the HTTP server layer for an S3-compatible object storage API.
/// It is built on top of `axum` and exposes the core `ObjectStorage` operations
/// as S3-style REST endpoints.
///
/// # Planned Endpoints
///
/// | S3 Operation      | HTTP Method | Path Pattern       | Handler           |
/// |-------------------|-------------|--------------------|-------------------|
/// | PutBucket         | PUT         | `/` (with `?bucket`) | `create_bucket`   |
/// | ListBuckets       | GET         | `/`               | `list_buckets`    |
/// | PutObject         | PUT         | `/:bucket/:key`    | `put_object`      |
/// | GetObject         | GET         | `/:bucket/:key`    | `get_object`      |
/// | DeleteObject      | DELETE      | `/:bucket/:key`    | `delete_object`   |
/// | HeadObject        | HEAD        | `/:bucket/:key`    | `head_object`     |
/// | ListObjects       | GET         | `/:bucket`         | `list_objects`    |
/// | GetBucketAcl      | GET         | `/:bucket`         | `get_bucket_acl`  |
/// | PutBucketAcl      | PUT         | `/:bucket`         | `put_bucket_acl`  |
/// | GetObjectAcl      | GET         | `/:bucket/:key`    | `get_object_acl`  |
/// | PutObjectAcl      | PUT         | `/:bucket/:key`    | `put_object_acl`  |
/// | GetBucketVersioning | GET       | `/:bucket`         | `get_bucket_versioning` |
/// | PutBucketVersioning | PUT       | `/:bucket`         | `put_bucket_versioning` |

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, head, put},
    Json, Router,
};
use std::sync::Arc;

use crate::api::ObjectStorage;
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
        // Bucket operations
        .route("/", put(create_bucket))
        .route("/", get(list_buckets))
        .route("/:bucket", get(list_objects))
        .route("/:bucket", delete(delete_bucket))
        .route("/:bucket", put(create_bucket))
        // Object operations
        .route("/:bucket/:key*", put(put_object))
        .route("/:bucket/:key*", get(get_object))
        .route("/:bucket/:key*", delete(delete_object))
        .route("/:bucket/:key*", head(head_object))
        .with_state(state)
}

// =============================================================================
// Error Handling
// =============================================================================

/// Convert `StorageError` into an Axum-compatible HTTP response.
fn storage_error_to_response(err: StorageError) -> impl IntoResponse {
    match &err {
        StorageError::NotFound(key) => {
            (StatusCode::NOT_FOUND, format!("Not Found: {key}"))
        }
        StorageError::ChecksumMismatch { expected, actual } => {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Checksum mismatch: expected {expected}, got {actual}"),
            )
        }
        StorageError::VersionConflict => {
            (
                StatusCode::CONFLICT,
                "Version conflict: concurrent write detected".to_string(),
            )
        }
        _ => {
            tracing::error!("Storage error: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            )
        }
    }
}

// =============================================================================
// Bucket Handlers (stubs)
// =============================================================================

/// PUT / → CreateBucket
///
/// Create a new bucket with the given name.
async fn create_bucket(
    State(state): State<S3AppState>,
    // bucket name extracted from path or header
) -> impl IntoResponse {
    // TODO: Extract bucket name from path/header
    // TODO: Validate bucket name
    // TODO: Create bucket metadata in meta_store
    (
        StatusCode::NOT_IMPLEMENTED,
        "CreateBucket not yet implemented",
    )
}

/// GET / → ListBuckets
///
/// List all buckets owned by this storage instance.
async fn list_buckets(
    State(state): State<S3AppState>,
) -> impl IntoResponse {
    // TODO: Scan meta_store for all buckets
    // TODO: Return ListBucketsResponse
    let response = ListBucketsResponse {
        buckets: Vec::new(),
    };
    (StatusCode::OK, Json(response))
}

/// DELETE /:bucket → DeleteBucket
///
/// Delete an empty bucket.
async fn delete_bucket(
    State(state): State<S3AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    // TODO: Check bucket is empty
    // TODO: Remove bucket metadata
    (
        StatusCode::NOT_IMPLEMENTED,
        format!("DeleteBucket '{bucket}' not yet implemented"),
    )
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
) -> impl IntoResponse {
    // TODO: Validate bucket exists
    // TODO: Call storage.list() with appropriate parameters
    // TODO: Build and return ListObjectsResponse
    let response = ListObjectsResponse {
        name: bucket,
        prefix: params.prefix.clone(),
        marker: params.marker.clone(),
        next_marker: None,
        max_keys: params.max_keys.unwrap_or(1000) as usize,
        is_truncated: false,
        contents: Vec::new(),
    };
    (StatusCode::OK, Json(response))
}

/// PUT /:bucket/:key → PutObject
///
/// Upload an object to a bucket. Supports both single-shot and streaming uploads.
async fn put_object(
    State(state): State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
    // body: Bytes,  // For future: use `axum::body::Body` for streaming
) -> impl IntoResponse {
    // TODO: Validate bucket exists
    // TODO: Extract request body (support streaming for large objects)
    // TODO: Call storage.put(&key, &data)
    // TODO: Return ETag and version token
    (
        StatusCode::NOT_IMPLEMENTED,
        format!("PutObject '{bucket}/{key}' not yet implemented"),
    )
}

/// GET /:bucket/:key → GetObject
///
/// Retrieve an object from a bucket.
async fn get_object(
    State(state): State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    // TODO: Validate bucket exists
    // TODO: Call storage.get(&key, None)
    // TODO: Return body with appropriate headers (Content-Length, ETag, etc.)
    (
        StatusCode::NOT_IMPLEMENTED,
        format!("GetObject '{bucket}/{key}' not yet implemented"),
    )
}

/// DELETE /:bucket/:key → DeleteObject
///
/// Delete an object from a bucket.
async fn delete_object(
    State(state): State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    // TODO: Validate bucket exists
    // TODO: Call storage.delete(&key)
    (
        StatusCode::NOT_IMPLEMENTED,
        format!("DeleteObject '{bucket}/{key}' not yet implemented"),
    )
}

/// HEAD /:bucket/:key → HeadObject
///
/// Retrieve metadata about an object without downloading its body.
async fn head_object(
    State(state): State<S3AppState>,
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
async fn get_bucket_acl(
    State(state): State<S3AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let _ = state;
    let _ = bucket;
    (
        StatusCode::NOT_IMPLEMENTED,
        "GetBucketAcl not yet implemented",
    )
}

/// PUT /:bucket?acl → PutBucketAcl
async fn put_bucket_acl(
    State(state): State<S3AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let _ = state;
    let _ = bucket;
    (
        StatusCode::NOT_IMPLEMENTED,
        "PutBucketAcl not yet implemented",
    )
}

/// GET /:bucket/:key?acl → GetObjectAcl
async fn get_object_acl(
    State(state): State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let _ = state;
    let _ = bucket;
    let _ = key;
    (
        StatusCode::NOT_IMPLEMENTED,
        "GetObjectAcl not yet implemented",
    )
}

/// PUT /:bucket/:key?acl → PutObjectAcl
async fn put_object_acl(
    State(state): State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let _ = state;
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
async fn get_bucket_versioning(
    State(state): State<S3AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let _ = state;
    let _ = bucket;
    (
        StatusCode::NOT_IMPLEMENTED,
        "GetBucketVersioning not yet implemented",
    )
}

/// PUT /:bucket?versioning → PutBucketVersioning
async fn put_bucket_versioning(
    State(state): State<S3AppState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let _ = state;
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
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use axum::Router;
    use crate::config::Config;
    use crate::api::ObjectStorage;
    use tempfile::TempDir;
    use tower::util::ServiceExt;

    fn make_test_config(tmp: &TempDir) -> Config {
        Config {
            base_path: tmp.path().display().to_string(),
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
}
