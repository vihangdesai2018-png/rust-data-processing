//! Core data model types for ingestion.
//!
//! This crate ingests supported formats into an in-memory [`DataSet`], using a user-provided
//! [`Schema`] (a list of typed [`Field`]s).

/// Logical data type for a schema field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    /// 64-bit signed integer.
    Int64,
    /// 64-bit floating point number.
    Float64,
    /// Boolean.
    Bool,
    /// UTF-8 string.
    Utf8,
}

/// A single named, typed field in a [`Schema`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    /// Field/column name.
    pub name: String,
    /// Field data type.
    pub data_type: DataType,
}

impl Field {
    /// Create a new field.
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}

/// A list of fields describing the expected shape of incoming data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    /// Ordered list of fields.
    pub fields: Vec<Field>,
}

impl Schema {
    /// Create a new schema from fields.
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    /// Iterate field names in order.
    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|f| f.name.as_str())
    }

    /// Returns the index of a field by name, if present.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }
}

/// A single typed value in a [`DataSet`].
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Missing/empty value.
    Null,
    /// 64-bit signed integer.
    Int64(i64),
    /// 64-bit float.
    Float64(f64),
    /// Boolean.
    Bool(bool),
    /// UTF-8 string.
    Utf8(String),
}

/// In-memory tabular dataset.
///
/// Rows are stored as `Vec<Vec<Value>>` in the same order as the [`Schema`] fields.
#[derive(Debug, Clone, PartialEq)]
pub struct DataSet {
    /// Schema describing row shape.
    pub schema: Schema,
    /// Row-major value storage.
    pub rows: Vec<Vec<Value>>,
}

impl DataSet {
    /// Create a dataset from schema and rows.
    pub fn new(schema: Schema, rows: Vec<Vec<Value>>) -> Self {
        Self { schema, rows }
    }

    /// Number of rows in the dataset.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Create a new dataset containing only rows that match `predicate`.
    ///
    /// The returned dataset preserves the original schema.
    pub fn filter_rows<F>(&self, mut predicate: F) -> Self
    where
        F: FnMut(&[Value]) -> bool,
    {
        let rows = self
            .rows
            .iter()
            .filter(|row| predicate(row.as_slice()))
            .cloned()
            .collect();
        Self {
            schema: self.schema.clone(),
            rows,
        }
    }

    /// Create a new dataset by applying `mapper` to every row.
    ///
    /// The returned dataset preserves the original schema.
    ///
    /// # Panics
    ///
    /// Panics if `mapper` returns a row with a different length than the schema field count.
    pub fn map_rows<F>(&self, mut mapper: F) -> Self
    where
        F: FnMut(&[Value]) -> Vec<Value>,
    {
        let expected_len = self.schema.fields.len();
        let rows = self
            .rows
            .iter()
            .map(|row| {
                let out = mapper(row.as_slice());
                assert!(
                    out.len() == expected_len,
                    "mapped row length {} does not match schema length {}",
                    out.len(),
                    expected_len
                );
                out
            })
            .collect();

        Self {
            schema: self.schema.clone(),
            rows,
        }
    }

    /// Reduce (fold) all rows into an accumulator value.
    ///
    /// This is similar to `Iterator::fold`, but provides each row as `&[Value]`.
    pub fn reduce_rows<A, F>(&self, init: A, mut reducer: F) -> A
    where
        F: FnMut(A, &[Value]) -> A,
    {
        self.rows
            .iter()
            .fold(init, |acc, row| reducer(acc, row.as_slice()))
    }
}
