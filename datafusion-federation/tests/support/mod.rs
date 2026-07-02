//! Shared test helpers for the federation integration tests.
//!
//! A [`RecordingSQLExecutor`] plays the role of a "remote database": it forwards
//! the pushed-down SQL to a local DataFusion [`SessionContext`] (backed by a CSV,
//! exactly like the examples) and records every query string it receives. Tests
//! can then assert on that recorded SQL to prove that compute was pushed down to
//! the remote instead of executed locally.

#![cfg(feature = "sql")]
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    catalog::SchemaProvider,
    execution::{
        context::{SessionContext, SessionState},
        options::CsvReadOptions,
        SessionStateBuilder,
    },
    physical_plan::{stream::RecordBatchStreamAdapter, PhysicalExpr, SendableRecordBatchStream},
    prelude::DataFrame,
    sql::unparser::dialect::{DefaultDialect, Dialect},
};
use futures::TryStreamExt;

use datafusion_federation::sql::{SQLExecutor, SQLFederationProvider, SQLSchemaProvider};

/// A CSV-backed [`SQLExecutor`] that records the SQL it is asked to execute.
///
/// The recorded queries are the exact strings federation unparsed and handed to
/// the remote engine, so asserting on them verifies what was pushed down.
pub struct RecordingSQLExecutor {
    name: &'static str,
    context: &'static str,
    session: Arc<SessionContext>,
    queries: Arc<Mutex<Vec<String>>>,
}

impl RecordingSQLExecutor {
    pub fn new(name: &'static str, context: &'static str, session: Arc<SessionContext>) -> Self {
        Self {
            name,
            context,
            session,
            queries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// A handle to the list of SQL strings this executor has received.
    pub fn queries(&self) -> Arc<Mutex<Vec<String>>> {
        self.queries.clone()
    }
}

#[async_trait]
impl SQLExecutor for RecordingSQLExecutor {
    fn name(&self) -> &str {
        self.name
    }

    fn compute_context(&self) -> Option<String> {
        // A unique, stable context so distinct remotes are never merged.
        Some(self.context.to_string())
    }

    fn execute(
        &self,
        sql: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn PhysicalExpr>],
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        self.queries.lock().unwrap().push(sql.to_string());

        let session = self.session.clone();
        let sql = sql.to_string();
        let future_stream = async move { session.sql(&sql).await?.execute_stream().await };
        let stream = futures::stream::once(future_stream).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> datafusion::error::Result<Vec<String>> {
        Err(datafusion::error::DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> datafusion::error::Result<SchemaRef> {
        let sql = format!("select * from {table_name} limit 1");
        let df = self.session.sql(&sql).await?;
        Ok(Arc::new(df.schema().as_arrow().clone()))
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(DefaultDialect {})
    }
}

/// Absolute path to a test CSV, resolved against the crate manifest dir so
/// tests work regardless of the working directory.
pub fn data_path(file: &str) -> String {
    format!("{}/tests/data/{file}", env!("CARGO_MANIFEST_DIR"))
}

/// Build a "remote" [`SessionContext`] with `csv` registered as `table_name`.
pub async fn remote_ctx(table_name: &str, csv: &str) -> Arc<SessionContext> {
    let ctx = Arc::new(SessionContext::new());
    ctx.register_csv(table_name, data_path(csv), CsvReadOptions::new())
        .await
        .expect("register csv");
    ctx
}

/// Register `schema` as the default schema of `state`.
pub fn overwrite_default_schema(state: &SessionState, schema: Arc<dyn SchemaProvider>) {
    let options = &state.config().options().catalog;
    let catalog = state
        .catalog_list()
        .catalog(options.default_catalog.as_str())
        .expect("default catalog");
    catalog
        .register_schema(options.default_schema.as_str(), schema)
        .expect("register schema");
}

/// Wrap `executor` in a federated schema provider exposing `tables`.
pub async fn schema_provider(
    executor: Arc<dyn SQLExecutor>,
    tables: &[&str],
) -> Arc<SQLSchemaProvider> {
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let tables: Vec<String> = tables.iter().map(|t| t.to_string()).collect();
    Arc::new(
        SQLSchemaProvider::new_with_tables(provider, tables)
            .await
            .expect("schema provider"),
    )
}

/// Run `query` against a federation-enabled context whose default schema is
/// `schema`, returning the collected result batches.
pub async fn run_federated(schema: Arc<dyn SchemaProvider>, query: &str) -> Vec<RecordBatch> {
    let state = datafusion_federation::default_session_state();
    overwrite_default_schema(&state, schema);
    let ctx = SessionContext::new_with_state(state);
    collect(ctx.sql(query).await.expect("plan query")).await
}

/// Collect a dataframe into batches.
pub async fn collect(df: DataFrame) -> Vec<RecordBatch> {
    df.collect().await.expect("collect")
}

/// Run `query` against a context whose default schema is `schema` but WITHOUT
/// the federation optimizer rule or query planner. This is the negative
/// control: the `FederatedTableProviderAdaptor` is never swapped for a
/// federation node, so scanning it fails. Returns the (expected) error.
pub async fn try_run_without_federation(
    schema: Arc<dyn SchemaProvider>,
    query: &str,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    let state = SessionStateBuilder::new().with_default_features().build();
    overwrite_default_schema(&state, schema);
    let ctx = SessionContext::new_with_state(state);
    ctx.sql(query).await?.collect().await
}

/// Total number of rows across `batches`.
pub fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// The concatenation of all SQL strings recorded so far, lowercased for
/// case-insensitive matching.
pub fn recorded_sql(queries: &Arc<Mutex<Vec<String>>>) -> String {
    queries.lock().unwrap().join("\n").to_lowercase()
}
