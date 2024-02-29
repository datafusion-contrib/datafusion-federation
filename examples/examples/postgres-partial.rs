use std::sync::Arc;
use tokio::task;

use datafusion::{
    catalog::schema::SchemaProvider,
    error::Result,
    execution::context::{SessionContext, SessionState},
};
use datafusion_federation::{FederatedQueryPlanner, FederationAnalyzerRule};
use datafusion_federation_sql::connectorx::CXExecutor;
use datafusion_federation_sql::{MultiSchemaProvider, SQLFederationProvider, SQLSchemaProvider};

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
    let pg_provider_1 = async_std::task::block_on(create_postgres_provider(vec!["class"], "conn1")).unwrap();
    let pg_provider_2 = async_std::task::block_on(create_postgres_provider(vec!["teacher"], "conn2")).unwrap();
    let provider = MultiSchemaProvider::new(vec![
        pg_provider_1,
        pg_provider_2,
    ]);

    overwrite_default_schema(&state, Arc::new(provider)).unwrap();

    // Run query
    let ctx = SessionContext::new_with_state(state);
    let query = r#"SELECT class.name AS classname, teacher.name AS teachername FROM class JOIN teacher ON class.id = teacher.class_id"#;
    let df = async_std::task::block_on(ctx.sql(query)).unwrap();

        df
    }).await.unwrap();

    task::spawn_blocking(move || async_std::task::block_on(df.show()))
        .await
        .unwrap()
}

async fn create_postgres_provider(
    known_tables: Vec<&str>,
    context: &str,
) -> Result<Arc<SQLSchemaProvider>> {
    let dsn = "postgresql://<username>:<password>@localhost:<port>/<dbname>".to_string();
    let known_tables: Vec<String> = known_tables.iter().map(|&x| x.into()).collect();
    let mut executor = CXExecutor::new(dsn)?;
    executor.context(context.to_string());
    let provider = Arc::new(SQLFederationProvider::new(Arc::new(executor)));
    Ok(Arc::new(
        SQLSchemaProvider::new_with_tables(provider, known_tables).await?,
    ))
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
