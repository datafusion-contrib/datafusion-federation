use std::{any::Any, sync::Arc};
use tokio::task;

use async_trait::async_trait;
use datafusion::{
    catalog::schema::SchemaProvider,
    datasource::TableProvider,
    error::Result,
    execution::context::{SessionContext, SessionState},
};
use datafusion_federation::{FederatedQueryPlanner, FederationAnalyzerRule};
use datafusion_federation_sql::{SQLFederationProvider, SQLSchemaProvider};
use datafusion_federation_sql::cx_executor::CXExecutor;

#[tokio::main]
async fn main() -> Result<()> {
    let state = SessionContext::new().state();
    // Register FederationAnalyzer
    // TODO: Interaction with other analyzers & optimizers.
    let state = state
        .add_analyzer_rule(Arc::new(FederationAnalyzerRule::new()))
        .with_query_planner(Arc::new(FederatedQueryPlanner::new()));

let df = task::spawn_blocking(move || {
    // Register schema
    let pg_provider = async_std::task::block_on(create_postgres_provider(vec!["person"], "conn1")).unwrap();
    let pg_col_provider = async_std::task::block_on(create_postgres_provider(vec!["job"], "conn2")).unwrap();
    let provider = MultiSchemaProvider::new(vec![
        pg_provider,
        pg_col_provider,
    ]);

    overwrite_default_schema(&state, Arc::new(provider)).unwrap();

    // Run query
    let ctx = SessionContext::new_with_state(state);
    let query = r#"SELECT person.name AS PersonName, job.title AS JobName, person.age AS Age
            FROM person JOIN job ON person.age = job.age"#;
        let df = async_std::task::block_on(ctx.sql(query)).unwrap();

        df
    }).await.unwrap();

    task::spawn_blocking(move || { async_std::task::block_on(df.show()) }).await.unwrap();
}

async fn create_sqlite_provider(
    known_tables: Vec<&str>,
    context: &str,
) -> Result<Arc<SQLSchemaProvider>> {
    let dsn = "sqlite://./examples/examples/chinook.sqlite".to_string();
    let known_tables: Vec<String> = known_tables.iter().map(|&x| x.into()).collect();
    let mut executor = CXExecutor::new(dsn)?;
    executor.context(context.to_string());
    let provider = Arc::new(SQLFederationProvider::new(Arc::new(executor)));
    Ok(Arc::new(
        SQLSchemaProvider::new(provider, known_tables).await?,
    ))
}

async fn create_postgres_provider(
    known_tables: Vec<&str>,
    context: &str,
) -> Result<Arc<SQLSchemaProvider>> {
    let dsn = "postgresql://user:password@localhost:5432/testdb".to_string();
    let known_tables: Vec<String> = known_tables.iter().map(|&x| x.into()).collect();
    let mut executor = CXExecutor::new(dsn)?;
    executor.context(context.to_string());
    let provider = Arc::new(SQLFederationProvider::new(Arc::new(executor)));
    Ok(Arc::new(
        SQLSchemaProvider::new(provider, known_tables).await?,
    ))
}

struct MultiSchemaProvider {
    children: Vec<Arc<dyn SchemaProvider>>,
}

impl MultiSchemaProvider {
    pub fn new(children: Vec<Arc<dyn SchemaProvider>>) -> Self {
        Self { children }
    }
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

#[async_trait]
impl SchemaProvider for MultiSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.children.iter().flat_map(|p| p.table_names()).collect()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        for child in &self.children {
            if let Some(table) = child.table(name).await {
                return Some(table);
            }
        }
        None
    }

    fn table_exist(&self, name: &str) -> bool {
        self.children.iter().any(|p| p.table_exist(name))
    }
}
