//! End-to-end federation tests.
//!
//! These mirror what the `df-csv` / `df-csv-advanced` examples demonstrate, but
//! turn it into assertions: each "remote database" is a local DataFusion context
//! that records the SQL it receives, so we can verify both that results are
//! correct and that compute (filters, limits, aggregates, joins) was actually
//! pushed down to the remote rather than run locally.

#![cfg(feature = "sql")]

mod support;

use support::{
    recorded_sql, remote_ctx, row_count, run_federated, schema_provider,
    try_run_without_federation, RecordingSQLExecutor,
};

/// `SELECT *` returns every row of the remote table.
#[tokio::test]
async fn select_star_returns_all_rows() {
    let ctx = remote_ctx("test", "test.csv").await;
    let executor = RecordingSQLExecutor::new("sqlite", "sqlite_exec", ctx);
    let queries = executor.queries();
    let schema = schema_provider(std::sync::Arc::new(executor), &["test"]).await;

    let batches = run_federated(schema, "SELECT * FROM test").await;

    assert_eq!(row_count(&batches), 3, "test.csv has 3 rows");
    // The scan was federated: the remote received a SELECT.
    assert!(
        recorded_sql(&queries).contains("select"),
        "remote should have received a query, got: {:?}",
        queries.lock().unwrap()
    );
}

/// A `WHERE` clause is pushed down to the remote as SQL, not applied locally.
#[tokio::test]
async fn filter_is_pushed_down() {
    let ctx = remote_ctx("test", "test.csv").await;
    let executor = RecordingSQLExecutor::new("sqlite", "sqlite_exec", ctx);
    let queries = executor.queries();
    let schema = schema_provider(std::sync::Arc::new(executor), &["test"]).await;

    let batches = run_federated(schema, "SELECT * FROM test WHERE bar > 1").await;

    assert_eq!(row_count(&batches), 2, "rows with bar > 1: b,2 and c,3");
    let sql = recorded_sql(&queries);
    assert!(sql.contains("where"), "filter should be pushed down: {sql}");
    assert!(sql.contains("bar"), "predicate column should appear: {sql}");
}

/// A `LIMIT` is pushed down to the remote.
#[tokio::test]
async fn limit_is_pushed_down() {
    let ctx = remote_ctx("test", "test.csv").await;
    let executor = RecordingSQLExecutor::new("sqlite", "sqlite_exec", ctx);
    let queries = executor.queries();
    let schema = schema_provider(std::sync::Arc::new(executor), &["test"]).await;

    let batches = run_federated(schema, "SELECT * FROM test LIMIT 1").await;

    assert_eq!(row_count(&batches), 1);
    assert!(
        recorded_sql(&queries).contains("limit"),
        "limit should be pushed down: {:?}",
        queries.lock().unwrap()
    );
}

/// An aggregation is pushed down to the remote.
#[tokio::test]
async fn aggregate_is_pushed_down() {
    let ctx = remote_ctx("test", "test.csv").await;
    let executor = RecordingSQLExecutor::new("sqlite", "sqlite_exec", ctx);
    let queries = executor.queries();
    let schema = schema_provider(std::sync::Arc::new(executor), &["test"]).await;

    let batches = run_federated(schema, "SELECT count(*) FROM test").await;

    assert_eq!(row_count(&batches), 1);
    assert!(
        recorded_sql(&queries).contains("count"),
        "aggregate should be pushed down: {:?}",
        queries.lock().unwrap()
    );
}

/// Negative control: the same query WITHOUT the federation rule fails to scan
/// and never reaches the remote. This is the opposite of `filter_is_pushed_down`
/// and proves that the remote is only queried because federation is active.
#[tokio::test]
async fn without_federation_scan_fails_and_remote_is_never_called() {
    let ctx = remote_ctx("test", "test.csv").await;
    let executor = RecordingSQLExecutor::new("sqlite", "sqlite_exec", ctx);
    let queries = executor.queries();
    let schema = schema_provider(std::sync::Arc::new(executor), &["test"]).await;

    let result = try_run_without_federation(schema, "SELECT * FROM test WHERE bar > 1").await;

    let err = result.expect_err("scan must fail without the federation rule");
    assert!(
        err.to_string().contains("cannot scan"),
        "expected FederatedTableProviderAdaptor scan error, got: {err}"
    );
    assert!(
        queries.lock().unwrap().is_empty(),
        "remote must not be queried without federation, got: {:?}",
        queries.lock().unwrap()
    );
}

/// A join across two independent remotes federates each side to its own remote,
/// mirroring the cross-database join in `df-csv-advanced`.
#[tokio::test]
async fn cross_provider_join() {
    use datafusion::execution::context::SessionContext;
    use datafusion_federation::sql::MultiSchemaProvider;
    use std::sync::Arc;

    // Remote #1: "sqlite" with table `test_sqlite`.
    let sqlite_ctx = remote_ctx("test_sqlite", "test.csv").await;
    let sqlite_exec = RecordingSQLExecutor::new("sqlite", "sqlite_exec", sqlite_ctx);
    let sqlite_queries = sqlite_exec.queries();
    let sqlite_schema = schema_provider(Arc::new(sqlite_exec), &["test_sqlite"]).await;

    // Remote #2: "postgres" with table `test_pg`.
    let pg_ctx = remote_ctx("test_pg", "test2.csv").await;
    let pg_exec = RecordingSQLExecutor::new("postgres", "postgres_exec", pg_ctx);
    let pg_queries = pg_exec.queries();
    let pg_schema = schema_provider(Arc::new(pg_exec), &["test_pg"]).await;

    let state = datafusion_federation::default_session_state();
    support::overwrite_default_schema(
        &state,
        Arc::new(MultiSchemaProvider::new(vec![sqlite_schema, pg_schema])),
    );
    let ctx = SessionContext::new_with_state(state);

    let batches = support::collect(
        ctx.sql("SELECT t.* FROM test_pg AS t JOIN test_sqlite AS a ON t.foo = a.foo")
            .await
            .expect("plan join"),
    )
    .await;

    // foo in {a,b,c} on both sides -> 3 matching rows.
    assert_eq!(row_count(&batches), 3);

    // Each remote received its own scan; neither remote saw the other's table.
    let sqlite_sql = recorded_sql(&sqlite_queries);
    let pg_sql = recorded_sql(&pg_queries);
    assert!(
        sqlite_sql.contains("test_sqlite"),
        "sqlite remote should scan its table: {sqlite_sql}"
    );
    assert!(
        pg_sql.contains("test_pg"),
        "postgres remote should scan its table: {pg_sql}"
    );
    assert!(
        !sqlite_sql.contains("test_pg"),
        "sqlite remote must not see the postgres table: {sqlite_sql}"
    );
}
