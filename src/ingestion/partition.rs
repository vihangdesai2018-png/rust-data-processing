//! Hive-style **partition path discovery** and helpers to resolve **glob patterns** or **explicit
//! file lists** — single-process only (no distributed coordinator).
//!
//! # Hive-style layout rules
//!
//! A common batch layout (e.g. Apache Hive / Spark) stores files under directories whose names are
//! `key=value` pairs, for example:
//!
//! ```text
//! warehouse/my_table/dt=2024-01-01/region=us/part-00000.csv
//! ```
//!
//! **Rules used here:**
//!
//! - Discovery starts at a **root directory** you provide.
//! - For each **file** under that root, the **parent path relative to root** is split into path
//!   components. **Every** directory component must match `key=value` where both sides are
//!   non-empty (split on the **first** `=`). The filename itself is not a partition segment.
//! - A file placed **directly** under `root` (no partition directories) has an empty partition
//!   prefix.
//! - If **any** directory component is **not** of the form `key=value`, that file is **skipped**
//!   (not returned). This avoids mis-classifying folders like `staging/` or `_temporary/`.
//! - This crate does **not** validate that partition keys match your schema; callers may attach
//!   [`PartitionSegment`]s as extra columns after ingest in a later pipeline step.
//!
//! # Glob and explicit lists
//!
//! - [`paths_from_glob`] expands a filesystem glob (e.g. `data/**/*.parquet`) to existing files.
//! - [`paths_from_explicit_list`] checks that each path exists and is a file, then returns them in
//!   order (deduplicated while preserving first occurrence).

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use glob::{glob, Pattern};
use walkdir::WalkDir;

use crate::error::{IngestionError, IngestionResult};

/// One hive-style directory segment `key=value`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionSegment {
    /// Partition column name (left of `=`).
    pub key: String,
    /// Partition value (right of `=`).
    pub value: String,
}

/// A data file discovered under a hive-style tree, with parsed partition segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionedFile {
    /// Absolute or normalized path to the file.
    pub path: PathBuf,
    /// Partition segments from the root down to the file's parent directory (order preserved).
    pub segments: Vec<PartitionSegment>,
}

/// Parse a single path component as `key=value`.
///
/// Returns `None` if there is no `=`, either side is empty, or the string is malformed.
pub fn parse_partition_segment(component: &str) -> Option<PartitionSegment> {
    let (k, v) = component.split_once('=')?;
    if k.is_empty() || v.is_empty() {
        return None;
    }
    Some(PartitionSegment {
        key: k.to_string(),
        value: v.to_string(),
    })
}

/// Parse every directory component of `relative_parent` as hive segments.
///
/// `relative_parent` should be the path of the parent directory **relative to the partition
/// root**, or empty if the file sits directly under the root.
///
/// Returns `None` if any component is not a valid `key=value` segment.
pub fn hive_segments_for_relative_parent(relative_parent: &Path) -> Option<Vec<PartitionSegment>> {
    let mut segments = Vec::new();
    for c in relative_parent.components() {
        let std::path::Component::Normal(part) = c else {
            continue;
        };
        let s = part.to_str()?;
        segments.push(parse_partition_segment(s)?);
    }
    Some(segments)
}

/// Discover files under `root` whose parent path (relative to `root`) consists only of hive-style
/// `key=value` directory segments.
///
/// - `root` must exist and be a directory.
/// - If `file_pattern` is `Some`, it is a [`glob::Pattern`] string matched against the path of each
///   file **relative to `root`** (use forward slashes in the pattern for portability, e.g.
///   `**/*.csv`).
/// - Results are sorted by [`Path`] for deterministic ordering.
pub fn discover_hive_partitioned_files(
    root: impl AsRef<Path>,
    file_pattern: Option<&str>,
) -> IngestionResult<Vec<PartitionedFile>> {
    let root = root.as_ref();
    if !root.is_dir() {
        return Err(IngestionError::SchemaMismatch {
            message: format!(
                "hive partition root must be an existing directory: {}",
                root.display()
            ),
        });
    }

    let pattern = match file_pattern {
        None => None,
        Some(p) => Some(
            Pattern::new(p).map_err(|e| IngestionError::SchemaMismatch {
                message: format!("invalid glob pattern '{p}': {e}"),
            })?,
        ),
    };

    let root = root.to_path_buf();
    let mut out = Vec::new();

    for entry in WalkDir::new(&root).follow_links(false).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let rel = match path.strip_prefix(&root) {
            Ok(r) => r.to_path_buf(),
            Err(_) => continue,
        };

        if let Some(ref pat) = pattern {
            if !pat.matches_path_with(&rel, glob::MatchOptions {
                case_sensitive: true,
                require_literal_separator: true,
                require_literal_leading_dot: false,
            }) {
                continue;
            }
        }

        let parent = rel.parent().unwrap_or_else(|| Path::new(""));
        if let Some(segments) = hive_segments_for_relative_parent(parent) {
            out.push(PartitionedFile {
                path: path.to_path_buf(),
                segments,
            });
        }
    }

    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// Expand a filesystem glob pattern and return existing regular files, sorted by path.
///
/// Uses the [`glob`] crate (shell-style patterns). Patterns are platform-specific; prefer explicit
/// paths in tests when possible.
pub fn paths_from_glob(pattern: &str) -> IngestionResult<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = Vec::new();
    for entry in glob(pattern).map_err(|e| IngestionError::SchemaMismatch {
        message: format!("invalid glob pattern '{pattern}': {e}"),
    })? {
        let p = entry.map_err(|e| IngestionError::SchemaMismatch {
            message: format!("glob expansion error for '{pattern}': {e}"),
        })?;
        if p.is_file() {
            out.push(p);
        }
    }

    out.sort();
    out.dedup();
    Ok(out)
}

/// Validate and return an explicit list of file paths (must each exist and be a file).
///
/// Duplicates are removed while preserving first occurrence order.
pub fn paths_from_explicit_list(paths: &[PathBuf]) -> IngestionResult<Vec<PathBuf>> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for p in paths {
        if !p.is_file() {
            return Err(IngestionError::SchemaMismatch {
                message: format!(
                    "explicit path is not an existing file: {}",
                    p.display()
                ),
            });
        }
        if seen.insert(p.clone()) {
            out.push(p.clone());
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_segment_happy() {
        let s = parse_partition_segment("dt=2024-01-01").unwrap();
        assert_eq!(s.key, "dt");
        assert_eq!(s.value, "2024-01-01");
    }

    #[test]
    fn parse_segment_rejects() {
        assert!(parse_partition_segment("nodash").is_none());
        assert!(parse_partition_segment("=v").is_none());
        assert!(parse_partition_segment("k=").is_none());
    }

    #[test]
    fn hive_segments_nested() {
        let p = Path::new("dt=2024-01-01").join("region=us");
        let segs = hive_segments_for_relative_parent(&p).unwrap();
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].key, "dt");
        assert_eq!(segs[1].key, "region");
    }
}
