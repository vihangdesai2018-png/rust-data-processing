use std::path::PathBuf;

use rust_data_processing::ingestion::{
    discover_hive_partitioned_files, paths_from_explicit_list, paths_from_glob, PartitionSegment,
};

fn fixture_root(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

#[test]
fn discover_hive_two_partitions_plus_root_file() {
    let root = fixture_root("hive_partitioned");
    let mut files = discover_hive_partitioned_files(&root, None).unwrap();
    assert_eq!(files.len(), 3);

    files.sort_by(|a, b| a.path.cmp(&b.path));

    let at_root = files.iter().find(|f| f.path.ends_with("at_root.csv")).unwrap();
    assert!(at_root.segments.is_empty());

    let us = files
        .iter()
        .find(|f| f.path.ends_with("region=us/events.csv"))
        .unwrap();
    assert_eq!(
        us.segments,
        vec![
            PartitionSegment {
                key: "dt".into(),
                value: "2024-01-01".into(),
            },
            PartitionSegment {
                key: "region".into(),
                value: "us".into(),
            },
        ]
    );

    let eu = files
        .iter()
        .find(|f| f.path.ends_with("region=eu/events.csv"))
        .unwrap();
    assert_eq!(eu.segments[1].value, "eu");
}

#[test]
fn discover_hive_with_glob_pattern() {
    let root = fixture_root("hive_partitioned");
    let files = discover_hive_partitioned_files(&root, Some("**/events.csv")).unwrap();
    assert_eq!(files.len(), 2);
    assert!(files.iter().all(|f| f.path.ends_with("events.csv")));
}

#[test]
fn discover_skips_non_hive_directories() {
    let root = fixture_root("hive_partitioned_skip");
    let files = discover_hive_partitioned_files(&root, None).unwrap();
    assert!(
        files.is_empty(),
        "staging/ is not hive key=value; file should be skipped"
    );
}

#[test]
fn discover_rejects_non_directory_root() {
    let f = fixture_root("hive_partitioned/at_root.csv");
    let err = discover_hive_partitioned_files(&f, None).unwrap_err();
    assert!(err.to_string().contains("directory"));
}

#[test]
fn paths_from_glob_finds_fixture_csvs() {
    let pat = fixture_root("hive_partitioned").join("**/*.csv");
    let pat = pat.to_string_lossy().replace('\\', "/");
    let paths = paths_from_glob(&pat).unwrap();
    assert!(paths.len() >= 3);
}

#[test]
fn paths_from_explicit_list_order_and_dedup() {
    let root = fixture_root("hive_partitioned");
    let a = root.join("at_root.csv");
    let b = root
        .join("dt=2024-01-01")
        .join("region=us")
        .join("events.csv");
    let list = vec![a.clone(), b.clone(), a.clone()];
    let paths = paths_from_explicit_list(&list).unwrap();
    assert_eq!(paths.len(), 2);
    assert_eq!(paths[0], a);
    assert_eq!(paths[1], b);
}

#[test]
fn paths_from_explicit_list_errors_on_missing() {
    let p = fixture_root("hive_partitioned").join("nope.csv");
    let err = paths_from_explicit_list(&[p]).unwrap_err();
    assert!(err.to_string().contains("not an existing file"));
}
