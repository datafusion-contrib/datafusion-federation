use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::SchemaProvider,
    error::{DataFusionError, Result},
    execution::{
        context::{SessionContext, SessionState},
        options::CsvReadOptions,
    },
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
    sql::unparser::dialect::{DefaultDialect, Dialect},
};
use datafusion_federation::sql::{SQLExecutor, SQLFederationProvider, SQLSchemaProvider};
use futures::TryStreamExt;

const CSV_PATH: &str = "./examples/test.csv";
const TABLE_NAME: &str = "test";

#[tokio::main]
async fn main() -> Result<()> {
    // Create a remote context
    let remote_ctx = Arc::new(SessionContext::new());

    // Registers a CSV file
    remote_ctx
        .register_csv(TABLE_NAME, CSV_PATH, CsvReadOptions::new())
        .await?;
    let known_tables: Vec<String> = [TABLE_NAME].iter().map(|&x| x.into()).collect();

    // Register schema
    let executor = Arc::new(InMemorySQLExecutor::new(remote_ctx));
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let schema_provider =
        Arc::new(SQLSchemaProvider::new_with_tables(provider, known_tables).await?);

    // Local context
    let state = datafusion_federation::default_session_state();
    overwrite_default_schema(&state, schema_provider)?;
    let ctx = SessionContext::new_with_state(state);

    // Run query
    let query = r#"SELECT * from test"#;
    let df = ctx.sql(query).await?;

    // let explain = df.clone().explain(true, false)?;
    // explain.show().await?;

    df.show().await
}

fn overwrite_default_schema(state: &SessionState, schema: Arc<dyn SchemaProvider>) -> Result<()> {
    let options = &state.config().options().catalog;
    let catalog = state
        .catalog_list()
        .catalog(options.default_catalog.as_str())
        .unwrap();

    catalog.register_schema(options.default_schema.as_str(), schema)?;

    Ok(())
}

pub struct InMemorySQLExecutor {
    session: Arc<SessionContext>,
}

impl InMemorySQLExecutor {
    pub fn new(session: Arc<SessionContext>) -> Self {
        Self { session }
    }
}

#[async_trait]
impl SQLExecutor for InMemorySQLExecutor {
    fn name(&self) -> &str {
        "in_memory_sql_executor"
    }

    fn compute_context(&self) -> Option<String> {
        None
    }

    fn execute(&self, sql: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream> {
        // Execute it using the remote datafusion session context
        let future_stream = _execute(self.session.clone(), sql.to_string());
        let stream = futures::stream::once(future_stream).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream,
        )))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        let sql = format!("select * from {table_name} limit 1");
        let df = self.session.sql(&sql).await?;
        let schema = df.schema().as_arrow().clone();
        Ok(Arc::new(schema))
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(DefaultDialect {})
    }
}

async fn _execute(ctx: Arc<SessionContext>, sql: String) -> Result<SendableRecordBatchStream> {
    ctx.sql(&sql).await?.execute_stream().await
}
