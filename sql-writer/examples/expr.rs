use std::sync::Arc;

use datafusion::{
    common::Column,
    logical_expr::{BinaryExpr, Operator},
    prelude::Expr,
    sql::{sqlparser::dialect::GenericDialect, TableReference},
};
use datafusion_sql_writer::from_df_expr;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example expression
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column {
            relation: Some(TableReference::bare("table_a")),
            name: "id".to_string(),
        })),
        op: Operator::Gt,
        right: Box::new(Expr::Column(Column {
            relation: Some(TableReference::bare("table_b")),
            name: "b".to_string(),
        })),
    });

    // datafusion::Expr -> sqlparser::ast
    let dialect = Arc::new(GenericDialect {});
    let ast = from_df_expr(&expr, dialect)?;

    // Get SQL string by formatting the AST
    let sql = format!("{}", ast);

    println!("{sql}");

    Ok(())
}
