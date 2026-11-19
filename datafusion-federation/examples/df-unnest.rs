mod shared;

use std::sync::Arc;

use datafusion::execution::SessionStateDefaults;
use datafusion::prelude::{CsvReadOptions, NdJsonReadOptions};
use datafusion::{
    execution::{context::SessionContext, session_state::SessionStateBuilder},
    optimizer::Optimizer,
};

use datafusion_federation::{
    sql::{MultiSchemaProvider, SQLFederationProvider, SQLSchemaProvider},
    FederatedQueryPlanner, FederationOptimizerRule,
};

use shared::{overwrite_default_schema, MockPostgresExecutor, MockSqliteExecutor};

const JSON_PATH_POSTGRES: &str = "./examples/data/unnest.ndjson";
const CSV_PATH_SQLITE: &str = "./examples/data/test.csv";
const TABLE_NAME_POSTGRES: &str = "arrays_pg";
const TABLE_NAME_SQLITE: &str = "items_sqlite";

#[tokio::main]
async fn main() {
    // This example demonstrates DataFusion Federation with cross-database JOIN and unnest operations.
    // The query demonstrates federation across two engines with an unnest operation:
    //
    // ```sql
    // SELECT unnest(a.qux) as item_id, i.foo as item_name
    // FROM arrays_pg as a
    // JOIN items_sqlite as i ON i.bar = unnest(a.qux)
    // ```
    //
    // - `arrays_pg` is a JSON file with array data (qux column contains [1,2,3] etc.)
    // - `items_sqlite` is a CSV file with item data (bar=1,2,3 etc., foo=names)
    // - The unnest operation is federated to PostgreSQL
    // - The JOIN happens in the main DataFusion engine
    // This validates that our Unnest fix enables proper federation of complex queries.

    /////////////////////
    // Remote sqlite DB
    /////////////////////
    let sqlite_remote_ctx = Arc::new(SessionContext::new());
    sqlite_remote_ctx
        .register_csv(TABLE_NAME_SQLITE, CSV_PATH_SQLITE, CsvReadOptions::new())
        .await
        .expect("Register sqlite csv file");

    let sqlite_known_tables: Vec<String> = [TABLE_NAME_SQLITE].iter().map(|&x| x.into()).collect();
    let sqlite_executor = Arc::new(MockSqliteExecutor::new(sqlite_remote_ctx));
    let sqlite_federation_provider = Arc::new(SQLFederationProvider::new(sqlite_executor));
    let sqlite_schema_provider = Arc::new(
        SQLSchemaProvider::new_with_tables(sqlite_federation_provider, sqlite_known_tables)
            .await
            .expect("Create sqlite schema provider"),
    );

    /////////////////////
    // Remote postgres DB
    /////////////////////
    let postgres_remote_ctx = Arc::new(SessionContext::new());
    postgres_remote_ctx
        .register_json(
            TABLE_NAME_POSTGRES,
            JSON_PATH_POSTGRES,
            NdJsonReadOptions {
                file_extension: ".ndjson",
                ..NdJsonReadOptions::default()
            },
        )
        .await
        .expect("Register postgres json file");

    let postgres_known_tables: Vec<String> =
        [TABLE_NAME_POSTGRES].iter().map(|&x| x.into()).collect();
    let postgres_executor = Arc::new(MockPostgresExecutor::new(postgres_remote_ctx));
    let postgres_federation_provider = Arc::new(SQLFederationProvider::new(postgres_executor));
    let postgres_schema_provider = Arc::new(
        SQLSchemaProvider::new_with_tables(postgres_federation_provider, postgres_known_tables)
            .await
            .expect("Create postgres schema provider"),
    );

    /////////////////////
    // Main (local) DB
    /////////////////////
    let mut rules = Optimizer::new().rules;
    rules.push(Arc::new(FederationOptimizerRule::new()));
    let state = SessionStateBuilder::new()
        .with_optimizer_rules(rules)
        .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
        .build();

    let schema_provider =
        MultiSchemaProvider::new(vec![sqlite_schema_provider, postgres_schema_provider]);
    overwrite_default_schema(&state, Arc::new(schema_provider))
        .expect("Overwrite the default schema");

    let ctx = SessionContext::new_with_state(state);
    SessionStateDefaults::register_builtin_functions(&mut ctx.state_ref().write());

    // Run a federated query with unnest and join
    // First unnest the arrays, then join with the items table
    let query = r#"
        SELECT unnest(a.qux) as item_value
            , a.foo as array_name
            , i.foo as item_name
        FROM arrays_pg as a JOIN items_sqlite as i ON a.id = i.bar
    "#;
    let df = ctx
        .sql(query)
        .await
        .expect("Create a dataframe from federated sql query");

    df.show()
        .await
        .expect("Execute the federated dataframe with unnest");
}
