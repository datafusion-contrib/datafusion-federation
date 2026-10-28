//! End-to-end federation tests.
//!
//! These mirror what the `df-csv` / `df-csv-advanced` examples demonstrate, but
//! turn it into assertions: each "remote database" is a local DataFusion context
//! that records the SQL it receives, so we can verify both that results are
//! correct and that compute (filters, limits, aggregates, joins) was actually
//! pushed down to the remote rather than run locally.

#![cfg(feature = "sql")]

mod support;

use std::sync::Arc;

use datafusion::{arrow::array::RecordBatch, prelude::SessionContext};
use datafusion_federation::sql::FILTERED_PLAN_ALIAS;
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

/// A plan that is only a table scan, as `SessionContext::table` builds it, is
/// federated to the remote instead of being scanned through the adaptor.
#[tokio::test]
async fn table_scan_alone_is_federated() {
    use datafusion::execution::context::SessionContext;
    use std::sync::Arc;

    let ctx = remote_ctx("test", "test.csv").await;
    let executor = RecordingSQLExecutor::new("sqlite", "sqlite_exec", ctx);
    let queries = executor.queries();
    let schema = schema_provider(Arc::new(executor), &["test"]).await;

    let state = datafusion_federation::default_session_state();
    support::overwrite_default_schema(&state, schema);
    let ctx = SessionContext::new_with_state(state);

    let batches = support::collect(ctx.table("test").await.expect("table")).await;

    assert_eq!(row_count(&batches), 3, "test.csv has 3 rows");
    assert!(
        recorded_sql(&queries).contains("test"),
        "remote should have received a scan of its table, got: {:?}",
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

/// Two remotes, one with `customers` and one with `orders`, and the SQL each received.
struct JoinFixture {
    ctx: SessionContext,
    customers_queries: Arc<std::sync::Mutex<Vec<String>>>,
    orders_queries: Arc<std::sync::Mutex<Vec<String>>>,
}

impl JoinFixture {
    async fn new() -> Self {
        use datafusion_federation::sql::MultiSchemaProvider;

        let orders_ctx = remote_ctx("orders", "orders.csv").await;
        let orders_exec = RecordingSQLExecutor::new("orders", "orders_exec", orders_ctx);
        let orders_queries = orders_exec.queries();
        let orders_schema = schema_provider(Arc::new(orders_exec), &["orders"]).await;

        let customers_ctx = remote_ctx("customers", "customers.csv").await;
        let customers_exec =
            RecordingSQLExecutor::new("customers", "customers_exec", customers_ctx);
        let customers_queries = customers_exec.queries();
        let customers_schema = schema_provider(Arc::new(customers_exec), &["customers"]).await;

        let state = datafusion_federation::default_session_state();
        support::overwrite_default_schema(
            &state,
            Arc::new(MultiSchemaProvider::new(vec![
                orders_schema,
                customers_schema,
            ])),
        );
        Self {
            ctx: SessionContext::new_with_state(state),
            customers_queries,
            orders_queries,
        }
    }

    async fn set(&self, option: &str, value: &str) {
        self.ctx
            .sql(&format!("SET {option} = {value}"))
            .await
            .expect("set option");
    }

    /// Runs `query`, returning its rows and the plan displayed after execution, when the
    /// dynamic filters are populated.
    async fn run(&self, query: &str) -> (Vec<RecordBatch>, String) {
        use datafusion::physical_plan::{collect, displayable};

        let df = self.ctx.sql(query).await.expect("plan query");
        let plan = df.create_physical_plan().await.expect("physical plan");
        let batches = collect(Arc::clone(&plan), self.ctx.task_ctx())
            .await
            .expect("collect");
        let display = displayable(plan.as_ref()).indent(true).to_string();
        (batches, display)
    }

    fn customers_sql(&self) -> String {
        recorded_sql(&self.customers_queries)
    }

    fn orders_sql(&self) -> String {
        recorded_sql(&self.orders_queries)
    }
}

/// The `WHERE` clause federation added to `sql` for the dynamic filters, if any.
fn dynamic_filter_sql(sql: &str) -> Option<&str> {
    sql.split_once(&format!("as {FILTERED_PLAN_ALIAS} where "))
        .map(|(_, predicate)| predicate)
}

/// The dynamic filter a hash join derives from its build side (customers with
/// `customer_type = 'bronze'`) is pushed into the SQL sent to the probe side (orders),
/// so the orders remote only returns orders of matching customers.
#[tokio::test]
async fn join_dynamic_filter_is_pushed_to_remote() {
    let fixture = JoinFixture::new().await;
    let (batches, plan) = fixture
        .run(
            "SELECT c.*, o.* FROM customers c \
             INNER JOIN orders o ON o.customer_id = c.customer_id \
             WHERE c.customer_type = 'bronze'",
        )
        .await;

    assert_eq!(row_count(&batches), 2, "customer 2 has two orders");

    let customers_sql = fixture.customers_sql();
    assert!(
        customers_sql.contains("customers") && !customers_sql.contains("orders"),
        "customers remote should only scan its table: {customers_sql}"
    );
    assert!(
        dynamic_filter_sql(&customers_sql).is_none(),
        "the build side gets no dynamic filter: {customers_sql}"
    );

    let orders_sql = fixture.orders_sql();
    assert!(
        orders_sql.contains("orders") && !orders_sql.contains("customers"),
        "orders remote should only scan its table: {orders_sql}"
    );
    let predicate = dynamic_filter_sql(&orders_sql)
        .unwrap_or_else(|| panic!("the orders query should be filtered: {orders_sql}"));
    assert!(
        predicate.contains(&format!("{FILTERED_PLAN_ALIAS}.customer_id >= 2"))
            && predicate.contains(&format!("{FILTERED_PLAN_ALIAS}.customer_id <= 2"))
            && predicate.contains(&format!("{FILTERED_PLAN_ALIAS}.customer_id in (2)")),
        "the filter should restrict orders to customer 2: {predicate}"
    );
    // Rerun the remote's query against the same data to count the rows it returned.
    let remote_sql = fixture.orders_queries.lock().unwrap()[0].clone();
    let remote = remote_ctx("orders", "orders.csv").await;
    let remote_rows = support::collect(remote.sql(&remote_sql).await.expect("remote query")).await;
    assert_eq!(
        row_count(&remote_rows),
        2,
        "the remote should only return the 2 orders of customer 2, not all 4: {remote_sql}"
    );
    assert!(
        plan.contains("filters=[DynamicFilter [ ") && !plan.contains("DynamicFilter [ empty ]"),
        "the populated dynamic filter should be displayed: {plan}"
    );
}

/// The join keys of several customers end up in the filter, and all their orders are returned.
#[tokio::test]
async fn join_dynamic_filter_with_several_build_rows() {
    let fixture = JoinFixture::new().await;
    let (batches, _) = fixture
        .run(
            "SELECT o.order_id FROM customers c \
             INNER JOIN orders o ON o.customer_id = c.customer_id \
             WHERE c.customer_type IN ('normal', 'bronze') \
             ORDER BY o.order_id",
        )
        .await;

    assert_eq!(
        row_count(&batches),
        3,
        "customers 1 and 2 have three orders"
    );
    let orders_sql = fixture.orders_sql();
    let predicate = dynamic_filter_sql(&orders_sql)
        .unwrap_or_else(|| panic!("the orders query should be filtered: {orders_sql}"));
    assert!(
        predicate.contains("customer_id"),
        "the filter should be on the join key: {predicate}"
    );
    assert!(
        !predicate.contains("3") && !predicate.contains("4"),
        "the filter should only allow customers 1 and 2: {predicate}"
    );
}

/// When no customer matches, the probe side can be skipped entirely: the remote gets `false`.
#[tokio::test]
async fn join_dynamic_filter_with_empty_build_side() {
    let fixture = JoinFixture::new().await;
    let (batches, _) = fixture
        .run(
            "SELECT c.*, o.* FROM customers c \
             INNER JOIN orders o ON o.customer_id = c.customer_id \
             WHERE c.customer_type = 'platinum'",
        )
        .await;

    assert_eq!(row_count(&batches), 0);
    // DataFusion may not even execute the probe side when the build side is empty.
    let orders_sql = fixture.orders_sql();
    if !orders_sql.is_empty() {
        assert_eq!(
            dynamic_filter_sql(&orders_sql),
            Some("false"),
            "an empty build side should filter out all orders: {orders_sql}"
        );
    }
}

/// With a single partition the join collects its build side instead of partitioning it, which
/// produces a dynamic filter without the per-partition `CASE`.
#[tokio::test]
async fn join_dynamic_filter_in_collect_left_mode() {
    let fixture = JoinFixture::new().await;
    fixture
        .set("datafusion.execution.target_partitions", "1")
        .await;
    let (batches, plan) = fixture
        .run(
            "SELECT c.*, o.* FROM customers c \
             INNER JOIN orders o ON o.customer_id = c.customer_id \
             WHERE c.customer_type = 'bronze'",
        )
        .await;

    assert!(plan.contains("mode=CollectLeft"), "{plan}");
    assert_eq!(row_count(&batches), 2);
    let orders_sql = fixture.orders_sql();
    let predicate = dynamic_filter_sql(&orders_sql)
        .unwrap_or_else(|| panic!("the orders query should be filtered: {orders_sql}"));
    assert!(predicate.contains("in (2)"), "{predicate}");
}

/// When the build side is too large for an `IN` list, the join uses a hash table lookup, which
/// has no SQL equivalent. Only the min/max bounds of the join keys are pushed down.
#[tokio::test]
async fn join_dynamic_filter_hash_lookup_pushes_only_bounds() {
    let fixture = JoinFixture::new().await;
    fixture
        .set(
            "datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values",
            "0",
        )
        .await;
    let (batches, plan) = fixture
        .run(
            "SELECT c.*, o.* FROM customers c \
             INNER JOIN orders o ON o.customer_id = c.customer_id \
             WHERE c.customer_type = 'bronze'",
        )
        .await;

    assert_eq!(row_count(&batches), 2);
    assert!(plan.contains("hash_lookup"), "{plan}");
    let orders_sql = fixture.orders_sql();
    let predicate = dynamic_filter_sql(&orders_sql)
        .unwrap_or_else(|| panic!("the bounds should still be pushed down: {orders_sql}"));
    assert!(
        predicate.contains(">= 2") && predicate.contains("<= 2") && !predicate.contains(" in "),
        "only the bounds should be pushed down: {predicate}"
    );
}

/// Disabling dynamic filters leaves the remote queries untouched.
#[tokio::test]
async fn join_without_dynamic_filter_pushdown() {
    let fixture = JoinFixture::new().await;
    fixture
        .set(
            "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
            "false",
        )
        .await;
    let (batches, plan) = fixture
        .run(
            "SELECT c.*, o.* FROM customers c \
             INNER JOIN orders o ON o.customer_id = c.customer_id \
             WHERE c.customer_type = 'bronze'",
        )
        .await;

    assert_eq!(row_count(&batches), 2);
    assert!(!plan.contains("DynamicFilter"), "{plan}");
    let orders_sql = fixture.orders_sql();
    assert!(
        dynamic_filter_sql(&orders_sql).is_none(),
        "the orders query should not be filtered: {orders_sql}"
    );
}

/// The preserved side of an outer join must not be narrowed down by the other side.
#[tokio::test]
async fn left_join_keeps_all_rows_of_the_preserved_side() {
    let fixture = JoinFixture::new().await;
    let (batches, _) = fixture
        .run(
            "SELECT o.order_id, c.customer_name FROM orders o \
             LEFT JOIN (SELECT * FROM customers WHERE customer_type = 'bronze') c \
             ON o.customer_id = c.customer_id",
        )
        .await;

    assert_eq!(row_count(&batches), 4, "every order is kept");
    let orders_sql = fixture.orders_sql();
    assert!(
        dynamic_filter_sql(&orders_sql).is_none(),
        "the preserved orders must not be filtered: {orders_sql}"
    );
}

/// The probe side is a federated `LIMIT`: the filter must apply to the limited rows, not narrow
/// down the rows the limit picks from.
#[tokio::test]
async fn join_dynamic_filter_is_applied_after_remote_limit() {
    let fixture = JoinFixture::new().await;
    let (batches, _) = fixture
        .run(
            "SELECT c.customer_id, o.order_id FROM customers c \
             INNER JOIN (SELECT * FROM orders ORDER BY order_id LIMIT 2) o \
             ON o.customer_id = c.customer_id \
             WHERE c.customer_type = 'bronze'",
        )
        .await;

    // The first two orders are 1 (customer 2) and 2 (customer 1).
    assert_eq!(row_count(&batches), 1, "only order 1 belongs to customer 2");
    let orders_sql = fixture.orders_sql();
    let (limited, _) = orders_sql
        .split_once(&format!("as {FILTERED_PLAN_ALIAS} where"))
        .unwrap_or_else(|| panic!("the orders query should be filtered: {orders_sql}"));
    assert!(
        limited.contains("limit 2"),
        "the limit should be inside the filtered derived table: {orders_sql}"
    );
}

/// A dynamic filter from a federated join is also displayed in `EXPLAIN ANALYZE`.
#[tokio::test]
async fn explain_analyze_shows_dynamic_filter() {
    let fixture = JoinFixture::new().await;
    let (batches, _) = fixture
        .run(
            "EXPLAIN ANALYZE SELECT c.*, o.* FROM customers c \
             INNER JOIN orders o ON o.customer_id = c.customer_id \
             WHERE c.customer_type = 'bronze'",
        )
        .await;

    let explained = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("format")
        .to_string();
    assert!(
        explained.contains("VirtualExecutionPlan name=orders")
            && explained.contains("filters=[DynamicFilter [ ")
            && !explained.contains("DynamicFilter [ empty ]"),
        "{explained}"
    );
}

/// The dynamic filter of a join on a string key is not pushed to the remote: DataFusion compares
/// strings byte by byte, while the remote may use a collation (e.g. a case-insensitive one) and
/// drop rows DataFusion would keep. DataFusion still applies the filter locally.
#[tokio::test]
async fn join_dynamic_filter_on_string_key_is_not_pushed_to_remote() {
    use datafusion::physical_plan::{collect, displayable};
    use datafusion_federation::sql::MultiSchemaProvider;

    let build_ctx = remote_ctx("build", "test.csv").await;
    let build_exec = RecordingSQLExecutor::new("build", "build_exec", build_ctx);
    let build_schema = schema_provider(Arc::new(build_exec), &["build"]).await;

    let probe_ctx = remote_ctx("probe", "test2.csv").await;
    let probe_exec = RecordingSQLExecutor::new("probe", "probe_exec", probe_ctx);
    let probe_queries = probe_exec.queries();
    let probe_schema = schema_provider(Arc::new(probe_exec), &["probe"]).await;

    let state = datafusion_federation::default_session_state();
    support::overwrite_default_schema(
        &state,
        Arc::new(MultiSchemaProvider::new(vec![build_schema, probe_schema])),
    );
    let ctx = SessionContext::new_with_state(state);

    let df = ctx
        .sql(
            "SELECT p.foo, p.bar FROM build b \
             INNER JOIN probe p ON p.foo = b.foo \
             WHERE b.bar = 2",
        )
        .await
        .expect("plan query");
    let plan = df.create_physical_plan().await.expect("physical plan");
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    let plan = displayable(plan.as_ref()).indent(true).to_string();

    assert_eq!(row_count(&batches), 1, "only `b` is on both sides");
    assert!(
        plan.contains("VirtualExecutionPlan name=probe")
            && plan.contains("filters=[DynamicFilter [ ")
            && !plan.contains("DynamicFilter [ empty ]"),
        "the probe side should get a populated dynamic filter: {plan}"
    );
    let probe_sql = recorded_sql(&probe_queries);
    assert!(
        probe_sql.contains("probe"),
        "the probe remote should scan its table: {probe_sql}"
    );
    assert!(
        dynamic_filter_sql(&probe_sql).is_none(),
        "the string comparisons should not be pushed down: {probe_sql}"
    );
}
