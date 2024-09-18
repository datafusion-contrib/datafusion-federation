use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_cast::cast;
use arrow_schema::{ArrowError, DataType, IntervalUnit, SchemaRef};

use super::{
    intervals_cast::{
        cast_interval_monthdaynano_to_daytime, cast_interval_monthdaynano_to_yearmonth,
    },
    lists_cast::{cast_string_to_fixed_size_list, cast_string_to_large_list, cast_string_to_list},
    struct_cast::cast_string_to_struct,
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    UnableToConvertRecordBatch { source: ArrowError },

    UnexpectedNumberOfColumns { expected: usize, found: usize },
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::UnableToConvertRecordBatch { source } => {
                write!(f, "Unable to convert record batch: {}", source)
            }
            Error::UnexpectedNumberOfColumns { expected, found } => {
                write!(
                    f,
                    "Unexpected number of columns. Expected: {}, Found: {}",
                    expected, found
                )
            }
        }
    }
}

/// Cast a given record batch into a new record batch with the given schema.
/// It assumes the record batch columns are correctly ordered.
#[allow(clippy::needless_pass_by_value)]
pub fn try_cast_to(record_batch: RecordBatch, expected_schema: SchemaRef) -> Result<RecordBatch> {
    let actual_schema = record_batch.schema();

    if actual_schema.fields().len() != expected_schema.fields().len() {
        return Err(Error::UnexpectedNumberOfColumns {
            expected: expected_schema.fields().len(),
            found: actual_schema.fields().len(),
        });
    }

    let cols = expected_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, expected_field)| {
            let record_batch_col = record_batch.column(i);

            match (record_batch_col.data_type(), expected_field.data_type()) {
                (DataType::Utf8, DataType::List(item_type)) => {
                    cast_string_to_list(&Arc::clone(record_batch_col), item_type)
                        .map_err(|e| Error::UnableToConvertRecordBatch { source: e })
                }
                (DataType::Utf8, DataType::LargeList(item_type)) => {
                    cast_string_to_large_list(&Arc::clone(record_batch_col), item_type)
                        .map_err(|e| Error::UnableToConvertRecordBatch { source: e })
                }
                (DataType::Utf8, DataType::FixedSizeList(item_type, value_length)) => {
                    cast_string_to_fixed_size_list(
                        &Arc::clone(record_batch_col),
                        item_type,
                        *value_length,
                    )
                    .map_err(|e| Error::UnableToConvertRecordBatch { source: e })
                }
                (DataType::Utf8, DataType::Struct(_)) => {
                    cast_string_to_struct(&Arc::clone(record_batch_col), expected_field.clone())
                        .map_err(|e| Error::UnableToConvertRecordBatch { source: e })
                }
                (
                    DataType::Interval(IntervalUnit::MonthDayNano),
                    DataType::Interval(IntervalUnit::YearMonth),
                ) => cast_interval_monthdaynano_to_yearmonth(&Arc::clone(record_batch_col))
                    .map_err(|e| Error::UnableToConvertRecordBatch { source: e }),
                (
                    DataType::Interval(IntervalUnit::MonthDayNano),
                    DataType::Interval(IntervalUnit::DayTime),
                ) => cast_interval_monthdaynano_to_daytime(&Arc::clone(record_batch_col))
                    .map_err(|e| Error::UnableToConvertRecordBatch { source: e }),
                _ => cast(&Arc::clone(record_batch_col), expected_field.data_type())
                    .map_err(|e| Error::UnableToConvertRecordBatch { source: e }),
            }
        })
        .collect::<Result<Vec<Arc<dyn Array>>>>()?;

    RecordBatch::try_new(expected_schema, cols)
        .map_err(|e| Error::UnableToConvertRecordBatch { source: e })
}

#[cfg(test)]
mod test {
    use super::*;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{Field, Schema, TimeUnit};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Utf8, false),
        ]))
    }

    fn to_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::LargeUtf8, false),
            Field::new("c", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]))
    }

    fn batch_input() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
                Arc::new(StringArray::from(vec![
                    "2024-01-13 03:18:09.000000",
                    "2024-01-13 03:18:09",
                    "2024-01-13 03:18:09.000",
                ])),
            ],
        )
        .expect("record batch should not panic")
    }

    #[test]
    fn test_string_to_timestamp_conversion() {
        let result = try_cast_to(batch_input(), to_schema()).expect("converted");
        assert_eq!(3, result.num_rows());
    }
}
