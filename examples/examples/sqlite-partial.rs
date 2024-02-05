use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::schema::SchemaProvider,
    datasource::TableProvider,
    error::Result,
    execution::context::{SessionContext, SessionState},
};
use datafusion_federation::{FederatedQueryPlanner, FederationAnalyzerRule};
use datafusion_federation_sql::{executor::CXExecutor, SQLFederationProvider, SQLSchemaProvider};

#[tokio::main]
async fn main() -> Result<()> {
    let state = SessionContext::new().state();
    // Register FederationAnalyzer
    // TODO: Interaction with other analyzers & optimizers.
    let state = state
        .add_analyzer_rule(Arc::new(FederationAnalyzerRule::new()))
        .with_query_planner(Arc::new(FederatedQueryPlanner::new()));

    // Register schema
    let provider = MultiSchemaProvider::new(vec![
        create_sqlite_provider(vec!["Artist"], "conn1").await?,
        create_sqlite_provider(vec!["Track", "Album"], "conn2").await?,
    ]);

    overwrite_default_schema(&state, Arc::new(provider))?;

    // Run query
    let ctx = SessionContext::new_with_state(state);
    let query = r#"SELECT
             t.TrackId,
             t.Name AS TrackName,
             a.Title AS AlbumTitle,
             ar.Name AS ArtistName
         FROM Track t
         JOIN Album a ON t.AlbumId = a.AlbumId
         JOIN Artist ar ON a.ArtistId = ar.ArtistId
         limit 10"#;
    let df = ctx.sql(query).await?;

    // let explain = df.clone().explain(true, false)?;
    // explain.show().await?;

    df.show().await
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
