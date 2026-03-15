//! CDC interface boundary (Phase 1 spike).
//!
//! This module intentionally **does not** depend on a specific CDC implementation crate.
//! It defines the public boundary types we would accept/produce if we add CDC in Phase 2.
//!
//! ## Example
//!
//! ```rust
//! use rust_data_processing::cdc::{CdcEvent, CdcOp, RowImage, SourceMeta, TableRef};
//! use rust_data_processing::types::Value;
//!
//! let ev = CdcEvent {
//!     meta: SourceMeta { source: Some("db".to_string()), checkpoint: None },
//!     table: TableRef::with_schema("public", "users"),
//!     op: CdcOp::Insert,
//!     before: None,
//!     after: Some(RowImage::new(vec![
//!         ("id".to_string(), Value::Int64(1)),
//!         ("name".to_string(), Value::Utf8("Ada".to_string())),
//!     ])),
//! };
//!
//! assert_eq!(ev.op, CdcOp::Insert);
//! ```

use crate::types::Value;

/// The operation represented by a CDC event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOp {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Identifies a table in a source database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub schema: Option<String>,
    pub name: String,
}

impl TableRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            schema: None,
            name: name.into(),
        }
    }

    pub fn with_schema(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: Some(schema.into()),
            name: name.into(),
        }
    }
}

/// A single row image.
///
/// We keep values as an ordered list (not a map) to preserve deterministic ordering and to allow
/// duplicate column names to be rejected by ingestion-time validation if desired.
#[derive(Debug, Clone, PartialEq)]
pub struct RowImage {
    pub values: Vec<(String, Value)>,
}

impl RowImage {
    pub fn new(values: Vec<(String, Value)>) -> Self {
        Self { values }
    }
}

/// A cursor/checkpoint used to resume CDC consumption.
///
/// This is intentionally opaque; different CDC implementations use different notions (LSN, GTID, etc.).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcCheckpoint(pub String);

/// Minimal metadata common across CDC sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceMeta {
    /// A human-friendly source identifier (e.g. connection name).
    pub source: Option<String>,
    /// Best-effort checkpoint token associated with this event.
    pub checkpoint: Option<CdcCheckpoint>,
}

/// A single change event.
#[derive(Debug, Clone, PartialEq)]
pub struct CdcEvent {
    pub meta: SourceMeta,
    pub table: TableRef,
    pub op: CdcOp,
    /// Before image (typically present for UPDATE/DELETE).
    pub before: Option<RowImage>,
    /// After image (typically present for INSERT/UPDATE).
    pub after: Option<RowImage>,
}

/// Batch-oriented CDC source boundary.
///
/// Decision (Phase 1): batch-first boundary avoids forcing async/runtime choice into the crate.
pub trait CdcSource {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Fetch the next batch of CDC events.
    ///
    /// - `Ok(None)` means "clean end" (source exhausted / stopped).
    /// - `Ok(Some(batch))` yields a non-empty batch.
    fn next_batch(&mut self) -> Result<Option<Vec<CdcEvent>>, Self::Error>;
}

