use std::{sync::Arc, time::Duration};

use datafusion::{
    catalog::schema::SchemaProvider,
    error::Result,
    execution::{
        context::{SessionContext, SessionState},
        options::CsvReadOptions,
    },
};
use datafusion_federation::{FederatedQueryPlanner, FederationAnalyzerRule};
use datafusion_federation_flight_sql::{executor::FlightSQLExecutor, server::FlightSqlService};
use datafusion_federation_sql::{SQLFederationProvider, SQLSchemaProvider};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn: String = "0.0.0.0:50051".to_string();
    let remote_ctx = SessionContext::new();
    remote_ctx
        .register_csv(
            "test",
            "./examples/examples/test.csv",
            CsvReadOptions::new(),
        )
        .await?;

    // Remote context
    tokio::spawn(async move {
        FlightSqlService::run_sql(dsn.clone(), remote_ctx.state())
            .await
            .unwrap();
    });

    // Wait for server to run
    sleep(Duration::from_secs(3)).await;

    // Local context
    let state = SessionContext::new().state();
    let known_tables: Vec<String> = ["test"].iter().map(|&x| x.into()).collect();

    // Register FederationAnalyzer
    // TODO: Interaction with other analyzers & optimizers.
    let state = state
        .add_analyzer_rule(Arc::new(FederationAnalyzerRule::new()))
        .with_query_planner(Arc::new(FederatedQueryPlanner::new()));

    // Register schema
    // TODO: table inference
    let dsn: String = "http://localhost:50051".to_string();
    let executor = Arc::new(FlightSQLExecutor::new(dsn));
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let schema_provider = Arc::new(SQLSchemaProvider::new(provider, known_tables).await?);
    overwrite_default_schema(&state, schema_provider)?;

    // Run query
    let ctx = SessionContext::new_with_state(state);
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
