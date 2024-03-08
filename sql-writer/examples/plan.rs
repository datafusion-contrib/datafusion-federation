use std::sync::Arc;

use datafusion::{
    execution::context::SessionContext, sql::sqlparser::dialect::GenericDialect,
    test_util::TestTableFactory,
};
use datafusion_sql_writer::from_df_plan;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example query
    let query = "select ta.id, tb.value from table_a ta join table_b tb on ta.id = tb.id;";

    // Create the DataFusion plan
    let dialect = Arc::new(GenericDialect {});
    let ctx = mock_ctx().await;
    let plan = ctx.sql(query).await.unwrap().into_unoptimized_plan();

    // datafusion::LogicalPlan -> sqlparser::ast
    let ast = from_df_plan(&plan, dialect)?;

    // Get SQL string by formatting the AST
    let sql = format!("{}", ast);

    println!("{sql}");
    Ok(())
}

async fn mock_ctx() -> SessionContext {
    let mut state = SessionContext::new().state();
    state
        .table_factories_mut()
        .insert("MOCKTABLE".to_string(), Arc::new(TestTableFactory {}));
    let ctx = SessionContext::new_with_state(state);

    ctx.sql("CREATE EXTERNAL TABLE table_a (id integer, value string) STORED AS MOCKTABLE LOCATION 'mock://path';").await.unwrap();
    ctx.sql("CREATE EXTERNAL TABLE table_b (id integer, value string) STORED AS MOCKTABLE LOCATION 'mock://path';").await.unwrap();
    ctx.sql("CREATE EXTERNAL TABLE table_c (id integer, value string) STORED AS MOCKTABLE LOCATION 'mock://path';").await.unwrap();

    ctx
}
