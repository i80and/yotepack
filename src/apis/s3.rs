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
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, head, put},
    Router,
};
use futures::stream::Stream;
use md5::{Digest, Md5};
use quick_xml::events::Event;
use quick_xml::writer::Writer;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;

// S3 XML namespace
const S3_NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

use crate::disk::BucketMeta;

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
use crate::errors::{StorageError, StorageResult};

// =============================================================================
// Streaming body: channels + futures Stream for zero-copy chunk streaming
// =============================================================================

/// A `futures::Stream` backed by a `tokio::sync::mpsc::Receiver`.
/// Yields `Vec<u8>` chunks for streaming large objects without
/// buffering the entire body in memory.
struct ReceiverStream {
    rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
}

impl Stream for ReceiverStream {
    type Item = Result<Vec<u8>, std::convert::Infallible>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            std::task::Poll::Ready(Some(data)) => std::task::Poll::Ready(Some(Ok(data))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

// =============================================================================
// Type Aliases
// =============================================================================

/// A single object entry in a list response.
#[derive(Debug, Clone)]
struct ContentEntry {
    key: String,
    last_modified: String,
    etag: String,
    size: usize,
}

// =============================================================================
// Request/Response Types (placeholders for future implementation)
// =============================================================================

/// Query parameters for `ListObjects`.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct ListObjectsQuery {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub marker: Option<String>,
    pub max_keys: Option<u32>,
}

/// Application state shared across request handlers.
#[derive(Clone)]
pub struct S3AppState {
    pub storage: Arc<ObjectStorage>,
}

// =============================================================================
// XML Response Builders (S3-compatible)
// =============================================================================

/// Helper to create an XML writer with S3 namespace.
fn xml_writer() -> Writer<Cursor<Vec<u8>>> {
    let mut writer = Writer::new(Cursor::new(Vec::new()));
    writer
        .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .unwrap();
    writer
}

/// ListBuckets XML response.
fn list_buckets_xml(buckets: &[BucketMeta]) -> String {
    let mut w = xml_writer();
    w.write_event(Event::Start(
        quick_xml::events::BytesStart::new("ListAllMyBucketsResult")
            .with_attributes([("xmlns", S3_NS)]),
    ))
    .unwrap();
    w.write_event(Event::Start(quick_xml::events::BytesStart::new("Owner")))
        .unwrap();
    w.write_event(Event::Start(quick_xml::events::BytesStart::new("ID")))
        .unwrap();
    w.write_event(Event::Text(quick_xml::events::BytesText::new("self")))
        .unwrap();
    w.write_event(Event::End(quick_xml::events::BytesEnd::new("ID")))
        .unwrap();
    w.write_event(Event::End(quick_xml::events::BytesEnd::new("Owner")))
        .unwrap();
    w.write_event(Event::Start(quick_xml::events::BytesStart::new("Buckets")))
        .unwrap();
    for b in buckets {
        w.write_event(Event::Start(quick_xml::events::BytesStart::new("Bucket")))
            .unwrap();
        w.write_event(Event::Start(quick_xml::events::BytesStart::new("Name")))
            .unwrap();
        w.write_event(Event::Text(quick_xml::events::BytesText::new(&b.name)))
            .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("Name")))
            .unwrap();
        w.write_event(Event::Start(quick_xml::events::BytesStart::new(
            "CreationDate",
        )))
        .unwrap();
        w.write_event(Event::Text(quick_xml::events::BytesText::new(
            &b.created_at,
        )))
        .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("CreationDate")))
            .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("Bucket")))
            .unwrap();
    }
    w.write_event(Event::End(quick_xml::events::BytesEnd::new("Buckets")))
        .unwrap();
    w.write_event(Event::End(quick_xml::events::BytesEnd::new(
        "ListAllMyBucketsResult",
    )))
    .unwrap();
    String::from_utf8(w.into_inner().into_inner()).unwrap()
}

/// Parameters for `list_objects_xml` serialization.
struct ListXmlParams<'a> {
    bucket_name: &'a str,
    prefix: &'a Option<String>,
    marker: &'a Option<String>,
    max_keys: usize,
    is_truncated: bool,
    next_marker: &'a Option<String>,
    contents: &'a [ContentEntry],
    common_prefixes: &'a [String],
}

/// ListObjects XML response.
fn list_objects_xml(params: &ListXmlParams) -> String {
    let mut w = xml_writer();
    w.write_event(Event::Start(
        quick_xml::events::BytesStart::new("ListBucketResult").with_attributes([("xmlns", S3_NS)]),
    ))
    .unwrap();
    w.write_event(Event::Start(quick_xml::events::BytesStart::new("Name")))
        .unwrap();
    w.write_event(Event::Text(quick_xml::events::BytesText::new(
        params.bucket_name,
    )))
    .unwrap();
    w.write_event(Event::End(quick_xml::events::BytesEnd::new("Name")))
        .unwrap();

    if let Some(p) = params.prefix {
        w.write_event(Event::Start(quick_xml::events::BytesStart::new("Prefix")))
            .unwrap();
        w.write_event(Event::Text(quick_xml::events::BytesText::new(p)))
            .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("Prefix")))
            .unwrap();
    }
    if let Some(m) = params.marker {
        w.write_event(Event::Start(quick_xml::events::BytesStart::new("Marker")))
            .unwrap();
        w.write_event(Event::Text(quick_xml::events::BytesText::new(m)))
            .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("Marker")))
            .unwrap();
    }
    w.write_event(Event::Start(quick_xml::events::BytesStart::new("MaxKeys")))
        .unwrap();
    w.write_event(Event::Text(quick_xml::events::BytesText::new(
        &params.max_keys.to_string(),
    )))
    .unwrap();
    w.write_event(Event::End(quick_xml::events::BytesEnd::new("MaxKeys")))
        .unwrap();
    w.write_event(Event::Start(quick_xml::events::BytesStart::new(
        "IsTruncated",
    )))
    .unwrap();
    w.write_event(Event::Text(quick_xml::events::BytesText::new(
        &params.is_truncated.to_string(),
    )))
    .unwrap();
    w.write_event(Event::End(quick_xml::events::BytesEnd::new("IsTruncated")))
        .unwrap();
    if let Some(nm) = params.next_marker {
        w.write_event(Event::Start(quick_xml::events::BytesStart::new(
            "NextMarker",
        )))
        .unwrap();
        w.write_event(Event::Text(quick_xml::events::BytesText::new(nm)))
            .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("NextMarker")))
            .unwrap();
    }

    for entry in params.contents {
        w.write_event(Event::Start(quick_xml::events::BytesStart::new("Contents")))
            .unwrap();
        w.write_event(Event::Start(quick_xml::events::BytesStart::new("Key")))
            .unwrap();
        w.write_event(Event::Text(quick_xml::events::BytesText::new(&entry.key)))
            .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("Key")))
            .unwrap();
        w.write_event(Event::Start(quick_xml::events::BytesStart::new(
            "LastModified",
        )))
        .unwrap();
        w.write_event(Event::Text(quick_xml::events::BytesText::new(
            &entry.last_modified,
        )))
        .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("LastModified")))
            .unwrap();
        w.write_event(Event::Start(quick_xml::events::BytesStart::new("ETag")))
            .unwrap();
        w.write_event(Event::Text(quick_xml::events::BytesText::new(&entry.etag)))
            .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("ETag")))
            .unwrap();
        w.write_event(Event::Start(quick_xml::events::BytesStart::new("Size")))
            .unwrap();
        w.write_event(Event::Text(quick_xml::events::BytesText::new(
            &entry.size.to_string(),
        )))
        .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("Size")))
            .unwrap();
        w.write_event(Event::Start(quick_xml::events::BytesStart::new(
            "StorageClass",
        )))
        .unwrap();
        w.write_event(Event::Text(quick_xml::events::BytesText::new("STANDARD")))
            .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("StorageClass")))
            .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("Contents")))
            .unwrap();
    }

    for cp in params.common_prefixes {
        w.write_event(Event::Start(quick_xml::events::BytesStart::new(
            "CommonPrefixes",
        )))
        .unwrap();
        w.write_event(Event::Start(quick_xml::events::BytesStart::new("Prefix")))
            .unwrap();
        w.write_event(Event::Text(quick_xml::events::BytesText::new(cp)))
            .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new("Prefix")))
            .unwrap();
        w.write_event(Event::End(quick_xml::events::BytesEnd::new(
            "CommonPrefixes",
        )))
        .unwrap();
    }

    w.write_event(Event::End(quick_xml::events::BytesEnd::new(
        "ListBucketResult",
    )))
    .unwrap();
    String::from_utf8(w.into_inner().into_inner()).unwrap()
}

/// S3 error XML response.
fn s3_error_xml(code: &str, message: &str) -> String {
    let mut w = xml_writer();
    w.write_event(Event::Start(quick_xml::events::BytesStart::new("Error")))
        .unwrap();
    w.write_event(Event::Start(quick_xml::events::BytesStart::new("Code")))
        .unwrap();
    w.write_event(Event::Text(quick_xml::events::BytesText::new(code)))
        .unwrap();
    w.write_event(Event::End(quick_xml::events::BytesEnd::new("Code")))
        .unwrap();
    w.write_event(Event::Start(quick_xml::events::BytesStart::new("Message")))
        .unwrap();
    w.write_event(Event::Text(quick_xml::events::BytesText::new(message)))
        .unwrap();
    w.write_event(Event::End(quick_xml::events::BytesEnd::new("Message")))
        .unwrap();
    w.write_event(Event::End(quick_xml::events::BytesEnd::new("Error")))
        .unwrap();
    String::from_utf8(w.into_inner().into_inner()).unwrap()
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
        .route("/:bucket/", get(list_objects))
        .route("/:bucket", delete(delete_bucket))
        .route("/:bucket/", delete(delete_bucket))
        .route("/:bucket", put(create_bucket))
        .route("/:bucket/", put(create_bucket))
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

/// Convert `StorageError` into an S3 XML error response.
fn storage_error_to_response(err: StorageError) -> (StatusCode, String, String) {
    let (status, code, message) = match &err {
        StorageError::NotFound(key) => (
            StatusCode::NOT_FOUND,
            "NoSuchKey".to_string(),
            format!("The specified key does not exist: {key}"),
        ),
        StorageError::BucketAlreadyExists(_name) => (
            StatusCode::CONFLICT,
            "BucketAlreadyExists".to_string(),
            "The requested bucket name is not available. The bucket namespace is shared across all users. Please choose a different name.".to_string(),
        ),
        StorageError::BucketNotFound(name) => (
            StatusCode::NOT_FOUND,
            "NoSuchBucket".to_string(),
            format!("The specified bucket does not exist: {name}"),
        ),
        StorageError::ChecksumMismatch { expected, actual } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError".to_string(),
            format!("Checksum mismatch: expected {expected}, got {actual}"),
        ),
        StorageError::VersionConflict => (
            StatusCode::CONFLICT,
            "InternalError".to_string(),
            "Version conflict: concurrent write detected".to_string(),
        ),
        StorageError::Transient(msg) if msg.contains("not empty") => (
            StatusCode::CONFLICT,
            "BucketNotEmpty".to_string(),
            "The bucket you tried to delete is not empty. Please delete all objects before deleting the bucket.".to_string(),
        ),
        StorageError::NoHealthyDisks => (
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable".to_string(),
            "No healthy metadata disks available".to_string(),
        ),
        _ => {
            tracing::error!("Storage error: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError".to_string(),
                "An internal error occurred".to_string(),
            )
        }
    };
    (status, code, message)
}

/// Convert a `StorageError` into an S3-compatible XML Response.
fn error_to_response(err: StorageError) -> Response {
    let (status, code, message) = storage_error_to_response(err);
    let body = s3_error_xml(&code, &message);
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/xml".parse().unwrap());
    let mut res = Response::new(axum::body::Body::from(body));
    *res.status_mut() = status;
    *res.headers_mut() = headers;
    res
}

// =============================================================================
// Bucket Handlers (stubs)
// =============================================================================

/// PUT /:bucket → CreateBucket
///
/// Create a new bucket with the given name.
async fn create_bucket(State(state): State<S3AppState>, Path(bucket): Path<String>) -> Response {
    // Strip trailing slash (s3cmd adds one)
    let bucket = bucket.strip_suffix('/').unwrap_or(&bucket).to_string();

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
            let body = s3_error_xml("BucketAlreadyExists", "The requested bucket name is not available. The bucket namespace is shared across all users. Please choose a different name.");
            let mut headers = HeaderMap::new();
            headers.insert("content-type", "application/xml".parse().unwrap());
            let mut res = Response::new(axum::body::Body::from(body));
            *res.status_mut() = StatusCode::CONFLICT;
            *res.headers_mut() = headers;
            res
        }
        Err(e) => error_to_response(e),
    }
}

/// GET / → ListBuckets
///
/// List all buckets owned by this storage instance.
async fn list_buckets(State(state): State<S3AppState>) -> Response {
    let buckets = match state.storage.list_buckets() {
        Ok(buckets) => buckets,
        Err(e) => return error_to_response(e),
    };

    let body = list_buckets_xml(&buckets);
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/xml".parse().unwrap());
    let mut res = Response::new(axum::body::Body::from(body));
    *res.status_mut() = StatusCode::OK;
    *res.headers_mut() = headers;
    res
}

/// DELETE /:bucket → DeleteBucket
///
/// Delete an empty bucket. Returns 409 if the bucket is not empty.
async fn delete_bucket(State(state): State<S3AppState>, Path(bucket): Path<String>) -> Response {
    // Strip trailing slash (s3cmd adds one)
    let bucket = bucket.strip_suffix('/').unwrap_or(&bucket).to_string();
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
        Err(StorageError::NotFound(_)) => {
            let body = s3_error_xml(
                "NoSuchBucket",
                &format!("The specified bucket does not exist: {bucket}"),
            );
            let mut headers = HeaderMap::new();
            headers.insert("content-type", "application/xml".parse().unwrap());
            let mut res = Response::new(axum::body::Body::from(body));
            *res.status_mut() = StatusCode::NOT_FOUND;
            *res.headers_mut() = headers;
            res
        }
        Err(StorageError::Transient(msg)) if msg.contains("not empty") => {
            let body = s3_error_xml("BucketNotEmpty", "The bucket you tried to delete is not empty. Please delete all objects before deleting the bucket.");
            let mut headers = HeaderMap::new();
            headers.insert("content-type", "application/xml".parse().unwrap());
            let mut res = Response::new(axum::body::Body::from(body));
            *res.status_mut() = StatusCode::CONFLICT;
            *res.headers_mut() = headers;
            res
        }
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
    // Strip trailing slash (s3cmd adds one)
    let bucket = bucket.strip_suffix('/').unwrap_or(&bucket).to_string();
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
    // Key in entries is "ver:bucket/key\x1fversion", we strip "ver:" and "\x1fversion" to get "bucket/key"
    let mut latest: std::collections::HashMap<String, (String, VersionMeta)> =
        std::collections::HashMap::new();
    for (raw_key, meta) in entries {
        if meta.status != VersionStatus::Committed {
            continue;
        }
        // Strip "ver:" prefix -> "bucket/key\x1fversion"
        let internal_key = raw_key.strip_prefix("ver:").unwrap_or(&raw_key).to_string();
        // Strip "\x1fversion" suffix -> "bucket/key"
        let display_key = internal_key
            .split('\x1f')
            .next()
            .unwrap_or(&internal_key)
            .to_string();
        let existing = latest
            .entry(display_key.clone())
            .or_insert_with(|| (display_key, meta.clone()));
        if meta.version > existing.1.version {
            existing.1 = meta;
        }
    }

    // Collect as sorted vec of (display_key, meta)
    let mut sorted: Vec<_> = latest.into_values().collect();
    sorted.sort_by_key(|(_, m)| m.version);

    // Filter by marker (skip entries before the marker)
    if let Some(marker_str) = marker {
        sorted.retain(|(_, m)| m.version > u64::from_str_radix(marker_str, 16).unwrap_or(0));
    }

    // Determine truncation
    let is_truncated = sorted.len() > max_keys;
    let next_marker = if is_truncated {
        sorted
            .get(max_keys)
            .map(|(_, m)| format!("{:08}", m.version))
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
    let (contents, common_prefixes): (Vec<ContentEntry>, Vec<String>) = if delimiter.is_empty() {
        let contents: Vec<ContentEntry> = sorted
            .into_iter()
            .map(|(key, meta)| {
                // Strip bucket prefix from key: "bucket/key" -> "key"
                let display_key = key
                    .strip_prefix(&format!("{bucket}/"))
                    .unwrap_or(&key)
                    .to_string();
                ContentEntry {
                    key: display_key,
                    last_modified: meta.last_modified.clone(),
                    etag: format!("\"{:x}\"", meta.checksum),
                    size: meta.data_size,
                }
            })
            .collect();
        (contents, Vec::new())
    } else {
        // Group by common prefix using delimiter
        let mut groups: std::collections::BTreeMap<String, ()> = std::collections::BTreeMap::new();
        let mut contents: Vec<ContentEntry> = Vec::new();

        for (key, meta) in sorted {
            // key is "bucket/key", compute relative display key
            let display_key = key
                .strip_prefix(&format!("{bucket}/"))
                .unwrap_or(&key)
                .to_string();
            // The suffix is everything after the user-provided prefix
            let suffix = display_key.strip_prefix(prefix).unwrap_or(&display_key);

            if let Some(pos) = suffix.find(delimiter) {
                // There's a delimiter in the suffix — this contributes to common prefixes
                let common = format!("{prefix}{}", &suffix[..=pos]);
                groups.insert(common, ());
            } else {
                // No delimiter — this is a leaf object
                contents.push(ContentEntry {
                    key: display_key,
                    last_modified: meta.last_modified.clone(),
                    etag: format!("\"{:x}\"", meta.checksum),
                    size: meta.data_size,
                });
            }
        }

        let common_prefixes: Vec<String> = groups.into_keys().collect();
        (contents, common_prefixes)
    };

    let body = list_objects_xml(&ListXmlParams {
        bucket_name: &bucket,
        prefix: &params.prefix,
        marker: &params.marker,
        max_keys: params.max_keys.unwrap_or(1000) as usize,
        is_truncated,
        next_marker: &next_marker,
        contents: &contents,
        common_prefixes: &common_prefixes,
    });

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/xml".parse().unwrap());
    let mut res = Response::new(axum::body::Body::from(body));
    *res.status_mut() = StatusCode::OK;
    *res.headers_mut() = headers;
    res
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
    let etag = format!("\"{:x}\"", hasher.finalize());

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
/// Retrieve an object from a bucket. Uses streaming reads to avoid
/// buffering large objects in memory.
async fn get_object(
    State(state): State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    // Validate bucket name
    if bucket.is_empty() || bucket.contains('/') || bucket.contains(':') {
        return error_to_response(StorageError::NotFound(format!(
            "Invalid bucket name: {bucket}"
        )));
    }

    // Build full object key
    let object_key = format!("{bucket}/{key}");

    // Check for Range header
    if let Some(range_header) = headers.get("range") {
        let range_str = match range_header.to_str() {
            Ok(s) => s,
            Err(_) => {
                return error_to_response(StorageError::InvalidRange(
                    "invalid Range header encoding".to_string(),
                ))
            }
        };

        // Parse "bytes=start-end"
        if !range_str.starts_with("bytes=") {
            return error_to_response(StorageError::InvalidRange(format!(
                "unsupported range format: {range_str}"
            )));
        }

        let range_body = &range_str[6..];
        let parts: Vec<&str> = range_body.splitn(2, '-').collect();
        if parts.len() != 2 {
            return error_to_response(StorageError::InvalidRange(format!(
                "malformed range: {range_str}"
            )));
        }

        let start: u64 = match parts[0].parse() {
            Ok(v) => v,
            Err(_) => {
                let msg = format!("invalid range start: {}", parts[0]);
                return error_to_response(StorageError::InvalidRange(msg));
            }
        };
        let end: u64 = match parts[1].parse() {
            Ok(v) => v,
            Err(_) => {
                let msg = format!("invalid range end: {}", parts[1]);
                return error_to_response(StorageError::InvalidRange(msg));
            }
        };

        // Read byte range using streaming read
        let meta = match state.storage.meta_store.read_version(&object_key) {
            Ok(m) => m,
            Err(e) => return error_to_response(e),
        };
        let total_size = meta.data_size as u64;

        // Spawn the streaming range read
        let storage_arc = Arc::clone(&state.storage);
        let object_key = object_key.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(4);

        tokio::spawn(async move {
            let mut cb =
                |chunk_idx: usize, data: &[u8], _corrections: Vec<_>| -> StorageResult<()> {
                    let _ = chunk_idx;
                    // Non-blocking send — if the receiver is full, just drop the chunk
                    let _ = tx.try_send(data.to_vec());
                    Ok(())
                };
            let _ = storage_arc
                .get_range_stream(&object_key, start, end, None, &mut cb)
                .await;
        });

        // Build headers (content-length for range is the total requested range size)
        let content_length = (end - start + 1) as u64;
        let etag = format!("\"{:x}\"", meta.checksum);

        let mut headers_map = HeaderMap::new();
        headers_map.insert(
            "content-length",
            content_length.to_string().parse().unwrap(),
        );
        headers_map.insert(
            "content-range",
            format!("bytes {start}-{end}/{total_size}").parse().unwrap(),
        );
        headers_map.insert("accept-ranges", "bytes".parse().unwrap());
        headers_map.insert("etag", etag.parse().unwrap());
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&meta.last_modified) {
            headers_map.insert(
                "last-modified",
                dt.format("%a, %d %b %Y %H:%M:%S GMT")
                    .to_string()
                    .parse()
                    .unwrap(),
            );
        }

        let body = Body::from_stream(ReceiverStream { rx });
        let mut res = Response::new(body);
        *res.status_mut() = StatusCode::PARTIAL_CONTENT;
        *res.headers_mut() = headers_map;
        return res;
    }

    // Full object read — streaming
    // First get metadata for headers
    let meta = match state.storage.meta_store.read_version(&object_key) {
        Ok(m) => m,
        Err(e) => return error_to_response(e),
    };
    let data_size = meta.data_size as u64;
    let etag = format!("\"{:x}\"", meta.checksum);

    let storage_arc = Arc::clone(&state.storage);
    let object_key_clone = object_key.clone();
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(4);

    // Spawn the streaming object read
    tokio::spawn(async move {
        let mut cb = |chunk_idx: usize, data: &[u8], _corrections: Vec<_>| -> StorageResult<()> {
            let _ = chunk_idx;
            // Non-blocking send — if the receiver is full, just drop the chunk
            let _ = tx.try_send(data.to_vec());
            Ok(())
        };
        let _ = storage_arc
            .get_stream(&object_key_clone, None, &mut cb)
            .await;
    });

    let mut headers_map = HeaderMap::new();
    headers_map.insert("content-length", data_size.to_string().parse().unwrap());
    headers_map.insert("etag", etag.parse().unwrap());
    headers_map.insert("accept-ranges", "bytes".parse().unwrap());
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&meta.last_modified) {
        headers_map.insert(
            "last-modified",
            dt.format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string()
                .parse()
                .unwrap(),
        );
    }

    let body = Body::from_stream(ReceiverStream { rx });
    let mut res = Response::new(body);
    *res.headers_mut() = headers_map;
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
    State(state): State<S3AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    // Validate bucket name
    if bucket.is_empty() || bucket.contains('/') || bucket.contains(':') {
        return error_to_response(StorageError::NotFound(format!(
            "Invalid bucket name: {bucket}"
        )));
    }

    // Build full object key
    let object_key = format!("{bucket}/{key}");

    // Retrieve the object metadata
    let meta = match state.storage.meta_store.read_version(&object_key) {
        Ok(m) => m,
        Err(e) => return error_to_response(e),
    };
    let data_size = meta.data_size;
    let etag = format!("\"{:x}\"", meta.checksum);

    let mut headers = HeaderMap::new();
    headers.insert("content-length", data_size.to_string().parse().unwrap());
    headers.insert("etag", etag.parse().unwrap());
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&meta.last_modified) {
        headers.insert(
            "last-modified",
            dt.format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string()
                .parse()
                .unwrap(),
        );
    }

    // HEAD returns headers but no body
    let mut res = Response::new(axum::body::Body::empty());
    *res.status_mut() = StatusCode::OK;
    *res.headers_mut() = headers;
    res
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
    use quick_xml::events::Event;
    use quick_xml::Reader;
    use tempfile::TempDir;
    use tower::util::ServiceExt;

    /// Parse an XML string and return a vec of (tag, text) pairs.
    fn xml_tags(xml: &str) -> Vec<(String, String)> {
        let mut reader = Reader::from_str(xml);
        let mut tags = Vec::new();
        let mut buf = Vec::new();
        let mut current_tag: Option<String> = None;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                    current_tag = Some(String::from_utf8_lossy(e.name().as_ref()).into_owned());
                }
                Ok(Event::Text(t)) => {
                    if let Some(ref tag) = current_tag {
                        let text = t.unescape().unwrap_or_default().into_owned();
                        if !text.is_empty() {
                            tags.push((tag.clone(), text));
                        }
                    }
                }
                Ok(Event::End(_)) => {
                    current_tag = None;
                }
                Ok(Event::Eof) => break,
                _ => {}
            }
            buf.clear();
        }
        tags
    }

    /// Count how many times a tag appears in the XML.
    fn xml_count(xml: &str, tag: &str) -> usize {
        xml_tags(xml).into_iter().filter(|(t, _)| *t == tag).count()
    }

    /// Parse ListBuckets XML and return bucket names.
    #[allow(dead_code)]
    fn parse_bucket_names(xml: &str) -> Vec<String> {
        xml_tags(xml)
            .into_iter()
            .filter(|(t, _)| *t == "Name")
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// Parse ListObjects XML and return object keys.
    #[allow(dead_code)]
    fn parse_object_keys(xml: &str) -> Vec<String> {
        xml_tags(xml)
            .into_iter()
            .filter(|(t, _)| *t == "Key")
            .map(|(_, v)| v.clone())
            .collect()
    }

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
            metadata_replicas: 0,
            disk_uuids: Vec::new(),
            compression_level: None,
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
        let xml = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(xml.contains("<Name>emptybucket</Name>"));
        assert_eq!(xml_count(&xml, "Key"), 0);
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
        let xml = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(xml.contains("<Name>mybucket</Name>"));
        // Should list all 4 objects
        assert_eq!(xml_count(&xml, "Key"), 4);
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
        let xml = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(xml.contains("<Name>testbucket</Name>"));
        assert_eq!(xml_count(&xml, "Key"), 2);
        // Verify all keys contain alpha/
        let keys = xml_tags(&xml)
            .into_iter()
            .filter(|(t, _)| *t == "Key")
            .map(|(_, v)| v)
            .collect::<Vec<_>>();
        assert!(keys.iter().all(|k| k.contains("alpha/")));
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
        let xml = String::from_utf8(body_bytes.to_vec()).unwrap();
        let names = parse_bucket_names(&xml);
        assert_eq!(names.len(), 0);
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
        let xml = String::from_utf8(body_bytes.to_vec()).unwrap();
        let names = parse_bucket_names(&xml);
        assert_eq!(names.len(), 3);

        // Verify they're sorted
        assert_eq!(names[0], "alpha");
        assert_eq!(names[1], "beta");
        assert_eq!(names[2], "gamma");

        // Verify each bucket has a creation date
        assert_eq!(xml_count(&xml, "CreationDate"), 3);
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
        let xml = String::from_utf8(body_bytes.to_vec()).unwrap();
        let names = parse_bucket_names(&xml);
        assert_eq!(names.len(), 0);
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
        let xml = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert_eq!(xml_count(&xml, "Key"), 1);
        assert!(xml.contains("bucket-a"));

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
        let xml = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert_eq!(xml_count(&xml, "Key"), 1);
        assert!(xml.contains("bucket-b"));

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

    #[tokio::test]
    async fn test_object_keys_with_colons() {
        let tmp = TempDir::new().unwrap();
        let config = make_test_config(&tmp);
        let storage = ObjectStorage::new(config).unwrap();
        let app = build_router(Arc::new(storage));

        // Create bucket
        let _response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("http://localhost/colonbucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Put objects with colons in the key
        let bodies = vec![
            ("key:with:colons.txt", b"data1".as_slice()),
            ("a:b/c:d.txt", b"data2".as_slice()),
            ("time:12:34:56", b"data3".as_slice()),
        ];

        for (key, data) in &bodies {
            let body = axum::body::Bytes::from(data.to_vec());
            let _response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("http://localhost/colonbucket/{key}"))
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Verify each object can be retrieved with correct content
        for (key, expected_data) in &bodies {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(format!("http://localhost/colonbucket/{key}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK, "Failed to get {key}");
            let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(
                body_bytes,
                axum::body::Bytes::from(expected_data.to_vec()),
                "Mismatch for {key}"
            );
        }

        // Verify listing works correctly
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("http://localhost/colonbucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let xml = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert_eq!(xml_count(&xml, "Key"), 3);
    }
}
