use std::sync::Arc;
use tokio::task;

use datafusion::{
    catalog::schema::SchemaProvider,
    error::Result,
    execution::context::{SessionContext, SessionState},
};
use datafusion_federation::{FederatedQueryPlanner, FederationAnalyzerRule};
use datafusion_federation_sql::{MultiSchemaProvider, SQLFederationProvider, SQLSchemaProvider};
use datafusion_federation_sql::connectorx::CXExecutor;

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
    let pg_provider = async_std::task::block_on(create_postgres_provider(vec!["class"], "conn1")).unwrap();
    let pg_col_provider = async_std::task::block_on(create_postgres_provider(vec!["teacher"], "conn2")).unwrap();
    let provider = MultiSchemaProvider::new(vec![
        pg_provider,
        pg_col_provider,
    ]);

    overwrite_default_schema(&state, Arc::new(provider)).unwrap();

    // Run query
    let ctx = SessionContext::new_with_state(state);
    let query = r#"SELECT class.name AS classname, teacher.name as teachername from class join teacher on class.id = teacher.class_id;"#;
        let df = async_std::task::block_on(ctx.sql(query)).unwrap();

        df
    }).await.unwrap();

    task::spawn_blocking(move || { async_std::task::block_on(df.show()) }).await.unwrap()
}

async fn create_postgres_provider(
    known_tables: Vec<&str>,
    context: &str,
) -> Result<Arc<SQLSchemaProvider>> {
    let dsn = "postgresql://suriya-retake:password@localhost:28816/pg_analytics".to_string();
    let known_tables: Vec<String> = known_tables.iter().map(|&x| x.into()).collect();
    let mut executor = CXExecutor::new(dsn)?;
    executor.context(context.to_string());
    let provider = Arc::new(SQLFederationProvider::new(Arc::new(executor)));
    Ok(Arc::new(
        SQLSchemaProvider::new_with_tables(provider, known_tables).await?,
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
