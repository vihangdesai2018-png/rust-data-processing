use rust_data_processing::pipeline::CastMode;
use rust_data_processing::transform::{TransformSpec, TransformStep};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

fn sample() -> DataSet {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("score", DataType::Int64),
        Field::new("name", DataType::Utf8),
    ]);
    DataSet::new(
        schema,
        vec![
            vec![Value::Int64(1), Value::Int64(10), Value::Utf8("Ada".to_string())],
            vec![Value::Int64(2), Value::Null, Value::Utf8("Grace".to_string())],
        ],
    )
}

#[test]
fn mapping_spec_rename_cast_fill_select_applies() {
    let ds = sample();

    let out_schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("score_i", DataType::Float64),
    ]);
    let spec = TransformSpec::new(out_schema).with_step(TransformStep::Rename {
        pairs: vec![("score".to_string(), "score_i".to_string())],
    })
    .with_step(TransformStep::Cast {
        column: "score_i".to_string(),
        to: DataType::Float64,
        mode: CastMode::Strict,
    })
    .with_step(TransformStep::FillNull {
        column: "score_i".to_string(),
        value: Value::Float64(0.0),
    })
    .with_step(TransformStep::Select {
        columns: vec!["id".to_string(), "score_i".to_string()],
    });

    let out = spec.apply(&ds).unwrap();
    assert_eq!(out.schema.field_names().collect::<Vec<_>>(), vec!["id", "score_i"]);
    assert_eq!(out.rows[0][1], Value::Float64(10.0));
    assert_eq!(out.rows[1][1], Value::Float64(0.0));
}

#[test]
fn mapping_spec_drop_and_with_literal_work() {
    let ds = sample();

    let out_schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("score", DataType::Int64),
        Field::new("tag", DataType::Utf8),
    ]);
    let spec = TransformSpec::new(out_schema)
        .with_step(TransformStep::WithLiteral {
            name: "tag".to_string(),
            value: Value::Utf8("v1".to_string()),
        })
        .with_step(TransformStep::Drop {
            columns: vec!["name".to_string()],
        })
        .with_step(TransformStep::Select {
            columns: vec!["id".to_string(), "score".to_string(), "tag".to_string()],
        });

    let out = spec.apply(&ds).unwrap();
    assert_eq!(out.schema.field_names().collect::<Vec<_>>(), vec!["id", "score", "tag"]);
    assert_eq!(out.rows[0][2], Value::Utf8("v1".to_string()));
}

