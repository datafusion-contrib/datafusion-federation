use std::sync::Arc;

use datafusion::{
    catalog::schema::SchemaProvider,
    error::Result,
    execution::context::{SessionContext, SessionState},
};
use datafusion_federation::{FederatedQueryPlanner, FederationAnalyzerRule};
use datafusion_federation_sql::{connectorx::CXExecutor, SQLFederationProvider, SQLSchemaProvider};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let dsn = "sqlite://./examples/examples/chinook.sqlite".to_string();
    let known_tables: Vec<String> = ["Track", "Album", "Artist"]
        .iter()
        .map(|&x| x.into())
        .collect();

    let state = SessionContext::new().state();

    // Register FederationAnalyzer
    // TODO: Interaction with other analyzers & optimizers.
    let state = state
        .add_analyzer_rule(Arc::new(FederationAnalyzerRule::new()))
        .with_query_planner(Arc::new(FederatedQueryPlanner::new()));

    // Register schema
    // TODO: table inference
    let executor = Arc::new(CXExecutor::new(dsn)?);
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let schema_provider =
        Arc::new(SQLSchemaProvider::new_with_tables(provider, known_tables).await?);
    overwrite_default_schema(&state, schema_provider)?;

    // Run query
    let ctx = SessionContext::new_with_state(state);
    let query = r#"SELECT Name, (SELECT Title FROM Album limit 1) FROM Artist limit 1"#;
    // let query = r#"SELECT ArtistId, Name, (SELECT Title FROM Album where ArtistId = a.ArtistId limit 1) FROM Artist a limit 1"#;
    let df = ctx.sql(query).await?;
    df.show().await?;

    // If the environment variable EXPLAIN is set, print the query plan
    if std::env::var("EXPLAIN").is_ok() {
        let explain_query = format!("EXPLAIN {query}");
        let df = ctx.sql(explain_query.as_str()).await?;

        df.show().await?;
    }

    Ok(())
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
