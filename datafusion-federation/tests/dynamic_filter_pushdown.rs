//! Joins across two remotes must return the same rows as plain DataFusion.
//!
//! When a join runs locally, DataFusion derives runtime (dynamic) filters from its build side
//! and pushes them to the probe side. Federation may use them to narrow down the SQL sent to the
//! probe-side remote, but only if the remote reads them exactly as DataFusion does: a stricter
//! filter at the remote drops rows the join needs, and SQL the remote cannot read fails the query.
//!
//! The join keys below use types whose values are easy to misrepresent in SQL.

#![cfg(feature = "sql")]

mod support;

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int64Array, RecordBatch, TimestampNanosecondArray,
};
use support::assert_federated_matches_local;

const JOIN: &str = "SELECT p.v, b.k FROM b JOIN p ON p.k = b.k ORDER BY p.v";

/// A table with join key `k` and a row number `v`.
fn table(k: ArrayRef) -> RecordBatch {
    let v: ArrayRef = Arc::new(Int64Array::from_iter_values(0..k.len() as i64));
    RecordBatch::try_from_iter([("k", k), ("v", v)]).expect("batch")
}

/// NaN has no standard SQL literal.
#[tokio::test]
async fn join_on_float64_key_with_nan() {
    let b = table(Arc::new(Float64Array::from(vec![1.0, f64::NAN])));
    let p = table(Arc::new(Float64Array::from(vec![1.0, f64::NAN, 2.0])));
    assert_federated_matches_local(&[("b", b), ("p", p)], JOIN).await;
}

/// Infinity has no standard SQL literal.
#[tokio::test]
async fn join_on_float64_key_with_infinity() {
    let b = table(Arc::new(Float64Array::from(vec![
        f64::NEG_INFINITY,
        1.0,
        f64::INFINITY,
    ])));
    let p = table(Arc::new(Float64Array::from(vec![
        f64::NEG_INFINITY,
        1.0,
        f64::INFINITY,
        2.0,
    ])));
    assert_federated_matches_local(&[("b", b), ("p", p)], JOIN).await;
}

/// A decimal literal such as `0.1` is read as a `Float64`, and `0.1f32` widened to a `Float64`
/// is `0.10000000149011612`, not `0.1`.
#[tokio::test]
async fn join_on_float32_key() {
    let b = table(Arc::new(Float32Array::from(vec![0.1])));
    let p = table(Arc::new(Float32Array::from(vec![0.1, 0.2])));
    assert_federated_matches_local(&[("b", b), ("p", p)], JOIN).await;
}

/// A timestamp with a time zone must keep denoting the same instant in SQL.
#[tokio::test]
async fn join_on_timestamp_with_time_zone_key() {
    let timestamps = |values: Vec<i64>| -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from(values).with_timezone("+08:00"))
    };
    let b = table(timestamps(vec![10_001]));
    let p = table(timestamps(vec![10_001, 20_002]));
    assert_federated_matches_local(&[("b", b), ("p", p)], JOIN).await;
}
