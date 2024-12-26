use std::{sync::Arc, time::Duration};

use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::execution::SessionStateBuilder;
use datafusion::{
    catalog::SchemaProvider,
    error::{DataFusionError, Result},
    execution::{
        context::{SessionContext, SessionState},
        options::CsvReadOptions,
    },
};
use datafusion_federation::{FederatedQueryPlanner, FederationAnalyzerRule};
use datafusion_federation_flight_sql::{executor::FlightSQLExecutor, server::FlightSqlService};
use datafusion_federation_sql::{SQLFederationProvider, SQLSchemaProvider};
use tokio::time::sleep;
use tonic::transport::Endpoint;

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
        FlightSqlService::new(remote_ctx.state())
            .serve(dsn.clone())
            .await
            .unwrap();
    });

    // Wait for server to run
    sleep(Duration::from_secs(3)).await;

    // Local context
    let known_tables: Vec<String> = ["test"].iter().map(|&x| x.into()).collect();

    // Register FederationAnalyzer
    // TODO: Interaction with other analyzers & optimizers.
    let mut state = SessionStateBuilder::new()
        .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
        .build();
    state.add_analyzer_rule(Arc::new(FederationAnalyzerRule::new()));

    // Register schema
    // TODO: table inference
    let dsn: String = "http://localhost:50051".to_string();
    let client = new_client(dsn.clone()).await?;
    let executor = Arc::new(FlightSQLExecutor::new(dsn, client));
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let schema_provider =
        Arc::new(SQLSchemaProvider::new_with_tables(provider, known_tables).await?);
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

/// Creates a new [FlightSqlServiceClient] for the passed endpoint. Completes the relevant auth configurations
/// or handshake as appropriate for the passed [FlightSQLAuth] variant.
async fn new_client(dsn: String) -> Result<FlightSqlServiceClient<tonic::transport::Channel>> {
    let endpoint = Endpoint::new(dsn).map_err(tx_error_to_df)?;
    let channel = endpoint.connect().await.map_err(tx_error_to_df)?;
    Ok(FlightSqlServiceClient::new(channel))
}

fn tx_error_to_df(err: tonic::transport::Error) -> DataFusionError {
    DataFusionError::External(format!("failed to connect: {err:?}").into())
}
