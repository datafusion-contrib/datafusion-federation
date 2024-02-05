use std::sync::Arc;

use datafusion::logical_expr::{JoinConstraint, JoinType, Like};
use datafusion::sql::sqlparser::ast::JoinOperator;
use datafusion::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
    sql::sqlparser::ast::{self, Expr as SQLExpr},
};

use datafusion::common::not_impl_err;
use datafusion::common::{Column, DFSchemaRef};
#[allow(unused_imports)]
use datafusion::logical_expr::aggregate_function;
use datafusion::logical_expr::expr::{
    Alias, BinaryExpr, Case, Cast, InList, ScalarFunction as DFScalarFunction, WindowFunction,
};
use datafusion::logical_expr::{Between, LogicalPlan, Operator};
use datafusion::prelude::Expr;

use crate::ast_builder::{
    BuilderError, QueryBuilder, RelationBuilder, SelectBuilder, TableRelationBuilder,
    TableWithJoinsBuilder,
};

pub fn query_to_sql(plan: &LogicalPlan) -> Result<ast::Statement> {
    match plan {
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::CrossJoin(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Statement(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Distinct(_) => {
            let mut query_builder = QueryBuilder::default();
            let mut select_builder = SelectBuilder::default();
            select_builder.push_from(TableWithJoinsBuilder::default());
            let mut relation_builder = RelationBuilder::default();
            select_to_sql(
                plan,
                &mut query_builder,
                &mut select_builder,
                &mut relation_builder,
            )?;

            let mut twj = select_builder.pop_from().unwrap();
            twj.relation(relation_builder);
            select_builder.push_from(twj);

            let body = ast::SetExpr::Select(Box::new(
                select_builder.build().map_err(builder_error_to_df)?,
            ));
            let query = query_builder
                .body(Box::new(body))
                .build()
                .map_err(builder_error_to_df)?;

            Ok(ast::Statement::Query(Box::new(query)))
        }
        LogicalPlan::Dml(_) => dml_to_sql(plan),
        LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_)
        | LogicalPlan::Extension(_)
        | LogicalPlan::Prepare(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::Copy(_)
        | LogicalPlan::DescribeTable(_)
        | LogicalPlan::Unnest(_) => Err(DataFusionError::NotImplemented(
            "Unsupported operator: {plan:?}".to_string(),
        )),
    }
}

fn select_to_sql(
    plan: &LogicalPlan,
    query: &mut QueryBuilder,
    select: &mut SelectBuilder,
    relation: &mut RelationBuilder,
) -> Result<()> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let mut builder = TableRelationBuilder::default();
            builder.name(ast::ObjectName(vec![new_ident(
                scan.table_name.table().to_string(),
            )]));
            relation.table(builder);

            Ok(())
        }
        LogicalPlan::Projection(p) => {
            let items = p
                .expr
                .iter()
                .map(|e| select_item_to_sql(e, p.input.schema(), 0).unwrap())
                .collect::<Vec<_>>();
            select.projection(items);

            select_to_sql(p.input.as_ref(), query, select, relation)
        }
        LogicalPlan::Filter(filter) => {
            let filter_expr = expr_to_sql(&filter.predicate, filter.input.schema(), 0)?;

            select.selection(Some(filter_expr));

            select_to_sql(filter.input.as_ref(), query, select, relation)
        }
        LogicalPlan::Limit(limit) => {
            if let Some(fetch) = limit.fetch {
                query.limit(Some(ast::Expr::Value(ast::Value::Number(
                    fetch.to_string(),
                    false,
                ))));
            }

            select_to_sql(limit.input.as_ref(), query, select, relation)
        }
        LogicalPlan::Sort(_sort) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::Aggregate(_agg) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::Distinct(_distinct) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::Join(join) => {
            match join.join_constraint {
                JoinConstraint::On => {}
                JoinConstraint::Using => {
                    return not_impl_err!("Unsupported join constraint: {:?}", join.join_constraint)
                }
            }

            // parse filter if exists
            let in_join_schema = join.left.schema().join(join.right.schema())?;
            let join_filter = match &join.filter {
                Some(filter) => Some(expr_to_sql(filter, &Arc::new(in_join_schema), 0)?),
                None => None,
            };

            // map join.on to `l.a = r.a AND l.b = r.b AND ...`
            let eq_op = ast::BinaryOperator::Eq;
            let join_on =
                join_conditions_to_sql(&join.on, eq_op, join.left.schema(), join.right.schema())?;

            // Merge `join_on` and `join_filter`
            let join_expr = match (join_filter, join_on) {
                (Some(filter), Some(on)) => Some(and_op_to_sql(filter, on)),
                (Some(filter), None) => Some(filter),
                (None, Some(on)) => Some(on),
                (None, None) => None,
            };
            let join_constraint = match join_expr {
                Some(expr) => ast::JoinConstraint::On(expr),
                None => ast::JoinConstraint::None,
            };

            let mut right_relation = RelationBuilder::default();

            select_to_sql(join.left.as_ref(), query, select, relation)?;
            select_to_sql(join.right.as_ref(), query, select, &mut right_relation)?;

            let ast_join = ast::Join {
                relation: right_relation.build().map_err(builder_error_to_df)?,
                join_operator: join_operator_to_sql(join.join_type, join_constraint),
            };
            let mut from = select.pop_from().unwrap();
            from.push_join(ast_join);
            select.push_from(from);

            Ok(())
        }
        LogicalPlan::SubqueryAlias(plan_alias) => {
            // Handle bottom-up to allocate relation
            select_to_sql(plan_alias.input.as_ref(), query, select, relation)?;

            relation.alias(Some(new_table_alias(plan_alias.alias.table().to_string())));

            Ok(())
        }
        LogicalPlan::Union(_union) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::Window(_window) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::Extension(_) => not_impl_err!("Unsupported operator: {plan:?}"),
        _ => not_impl_err!("Unsupported operator: {plan:?}"),
    }
}

fn select_item_to_sql(
    expr: &Expr,
    schema: &DFSchemaRef,
    col_ref_offset: usize,
) -> Result<ast::SelectItem> {
    match expr {
        Expr::Alias(Alias { expr, name, .. }) => {
            let inner = expr_to_sql(expr, schema, col_ref_offset)?;

            Ok(ast::SelectItem::ExprWithAlias {
                expr: inner,
                alias: new_ident(name.to_string()),
            })
        }
        _ => {
            let inner = expr_to_sql(expr, schema, col_ref_offset)?;

            Ok(ast::SelectItem::UnnamedExpr(inner))
        }
    }
}

fn expr_to_sql(expr: &Expr, _schema: &DFSchemaRef, _col_ref_offset: usize) -> Result<SQLExpr> {
    match expr {
        Expr::InList(InList {
            expr,
            list: _,
            negated: _,
        }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        Expr::ScalarFunction(DFScalarFunction { .. }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        Expr::Between(Between {
            expr,
            negated: _,
            low: _,
            high: _,
        }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        Expr::Column(col) => col_to_sql(col),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let l = expr_to_sql(left.as_ref(), _schema, 0)?;
            let r = expr_to_sql(right.as_ref(), _schema, 0)?;
            let op = op_to_sql(op)?;

            Ok(binary_op_to_sql(l, r, op))
        }
        Expr::Case(Case {
            expr,
            when_then_expr: _,
            else_expr: _,
        }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        Expr::Cast(Cast { expr, data_type: _ }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        Expr::Literal(value) => Ok(ast::Expr::Value(scalar_to_sql(value)?)),
        Expr::Alias(Alias { expr, name: _, .. }) => expr_to_sql(expr, _schema, _col_ref_offset),
        Expr::WindowFunction(WindowFunction {
            fun: _,
            args: _,
            partition_by: _,
            order_by: _,
            window_frame: _,
        }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        Expr::Like(Like {
            negated: _,
            expr,
            pattern: _,
            escape_char: _,
            case_insensitive: _,
        }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        _ => not_impl_err!("Unsupported expression: {expr:?}"),
    }
}

fn op_to_sql(op: &Operator) -> Result<ast::BinaryOperator> {
    match op {
        Operator::Eq => Ok(ast::BinaryOperator::Eq),
        Operator::NotEq => Ok(ast::BinaryOperator::NotEq),
        Operator::Lt => Ok(ast::BinaryOperator::Lt),
        Operator::LtEq => Ok(ast::BinaryOperator::LtEq),
        Operator::Gt => Ok(ast::BinaryOperator::Gt),
        Operator::GtEq => Ok(ast::BinaryOperator::GtEq),
        Operator::Plus => Ok(ast::BinaryOperator::Plus),
        Operator::Minus => Ok(ast::BinaryOperator::Minus),
        Operator::Multiply => Ok(ast::BinaryOperator::Multiply),
        Operator::Divide => Ok(ast::BinaryOperator::Divide),
        Operator::Modulo => Ok(ast::BinaryOperator::Modulo),
        Operator::And => Ok(ast::BinaryOperator::And),
        Operator::Or => Ok(ast::BinaryOperator::Or),
        Operator::IsDistinctFrom => not_impl_err!("unsupported operation: {op:?}"),
        Operator::IsNotDistinctFrom => not_impl_err!("unsupported operation: {op:?}"),
        Operator::RegexMatch => Ok(ast::BinaryOperator::PGRegexMatch),
        Operator::RegexIMatch => Ok(ast::BinaryOperator::PGRegexIMatch),
        Operator::RegexNotMatch => Ok(ast::BinaryOperator::PGRegexNotMatch),
        Operator::RegexNotIMatch => Ok(ast::BinaryOperator::PGRegexNotIMatch),
        Operator::BitwiseAnd => Ok(ast::BinaryOperator::BitwiseAnd),
        Operator::BitwiseOr => Ok(ast::BinaryOperator::BitwiseOr),
        Operator::BitwiseXor => Ok(ast::BinaryOperator::BitwiseXor),
        Operator::BitwiseShiftRight => Ok(ast::BinaryOperator::PGBitwiseShiftRight),
        Operator::BitwiseShiftLeft => Ok(ast::BinaryOperator::PGBitwiseShiftLeft),
        Operator::StringConcat => Ok(ast::BinaryOperator::StringConcat),
        Operator::AtArrow => not_impl_err!("unsupported operation: {op:?}"),
        Operator::ArrowAt => not_impl_err!("unsupported operation: {op:?}"),
    }
}

fn scalar_to_sql(v: &ScalarValue) -> Result<ast::Value> {
    match v {
        ScalarValue::Null => Ok(ast::Value::Null),
        ScalarValue::Boolean(Some(b)) => Ok(ast::Value::Boolean(b.to_owned())),
        ScalarValue::Boolean(None) => Ok(ast::Value::Null),
        ScalarValue::Float32(Some(f)) => Ok(ast::Value::Number(f.to_string(), false)),
        ScalarValue::Float32(None) => Ok(ast::Value::Null),
        ScalarValue::Float64(Some(f)) => Ok(ast::Value::Number(f.to_string(), false)),
        ScalarValue::Float64(None) => Ok(ast::Value::Null),
        ScalarValue::Decimal128(Some(_), ..) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Decimal128(None, ..) => Ok(ast::Value::Null),
        ScalarValue::Decimal256(Some(_), ..) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Decimal256(None, ..) => Ok(ast::Value::Null),
        ScalarValue::Int8(Some(i)) => Ok(ast::Value::Number(i.to_string(), false)),
        ScalarValue::Int8(None) => Ok(ast::Value::Null),
        ScalarValue::Int16(Some(i)) => Ok(ast::Value::Number(i.to_string(), false)),
        ScalarValue::Int16(None) => Ok(ast::Value::Null),
        ScalarValue::Int32(Some(i)) => Ok(ast::Value::Number(i.to_string(), false)),
        ScalarValue::Int32(None) => Ok(ast::Value::Null),
        ScalarValue::Int64(Some(i)) => Ok(ast::Value::Number(i.to_string(), false)),
        ScalarValue::Int64(None) => Ok(ast::Value::Null),
        ScalarValue::UInt8(Some(ui)) => Ok(ast::Value::Number(ui.to_string(), false)),
        ScalarValue::UInt8(None) => Ok(ast::Value::Null),
        ScalarValue::UInt16(Some(ui)) => Ok(ast::Value::Number(ui.to_string(), false)),
        ScalarValue::UInt16(None) => Ok(ast::Value::Null),
        ScalarValue::UInt32(Some(ui)) => Ok(ast::Value::Number(ui.to_string(), false)),
        ScalarValue::UInt32(None) => Ok(ast::Value::Null),
        ScalarValue::UInt64(Some(ui)) => Ok(ast::Value::Number(ui.to_string(), false)),
        ScalarValue::UInt64(None) => Ok(ast::Value::Null),
        ScalarValue::Utf8(Some(str)) => Ok(ast::Value::SingleQuotedString(str.to_string())),
        ScalarValue::Utf8(None) => Ok(ast::Value::Null),
        ScalarValue::LargeUtf8(Some(str)) => Ok(ast::Value::SingleQuotedString(str.to_string())),
        ScalarValue::LargeUtf8(None) => Ok(ast::Value::Null),
        ScalarValue::Binary(Some(_)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Binary(None) => Ok(ast::Value::Null),
        ScalarValue::FixedSizeBinary(..) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::LargeBinary(Some(_)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::LargeBinary(None) => Ok(ast::Value::Null),
        ScalarValue::FixedSizeList(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::List(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::LargeList(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Date32(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Date32(None) => Ok(ast::Value::Null),
        ScalarValue::Date64(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Date64(None) => Ok(ast::Value::Null),
        ScalarValue::Time32Second(Some(_t)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Time32Second(None) => Ok(ast::Value::Null),
        ScalarValue::Time32Millisecond(Some(_t)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Time32Millisecond(None) => Ok(ast::Value::Null),
        ScalarValue::Time64Microsecond(Some(_t)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Time64Microsecond(None) => Ok(ast::Value::Null),
        ScalarValue::Time64Nanosecond(Some(_t)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Time64Nanosecond(None) => Ok(ast::Value::Null),
        ScalarValue::TimestampSecond(Some(_ts), _) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::TimestampSecond(None, _) => Ok(ast::Value::Null),
        ScalarValue::TimestampMillisecond(Some(_ts), _) => {
            not_impl_err!("Unsupported scalar: {v:?}")
        }
        ScalarValue::TimestampMillisecond(None, _) => Ok(ast::Value::Null),
        ScalarValue::TimestampMicrosecond(Some(_ts), _) => {
            not_impl_err!("Unsupported scalar: {v:?}")
        }
        ScalarValue::TimestampMicrosecond(None, _) => Ok(ast::Value::Null),
        ScalarValue::TimestampNanosecond(Some(_ts), _) => {
            not_impl_err!("Unsupported scalar: {v:?}")
        }
        ScalarValue::TimestampNanosecond(None, _) => Ok(ast::Value::Null),
        ScalarValue::IntervalYearMonth(Some(_i)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::IntervalYearMonth(None) => Ok(ast::Value::Null),
        ScalarValue::IntervalDayTime(Some(_i)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::IntervalDayTime(None) => Ok(ast::Value::Null),
        ScalarValue::IntervalMonthDayNano(Some(_i)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::IntervalMonthDayNano(None) => Ok(ast::Value::Null),
        ScalarValue::DurationSecond(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::DurationSecond(None) => Ok(ast::Value::Null),
        ScalarValue::DurationMillisecond(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::DurationMillisecond(None) => Ok(ast::Value::Null),
        ScalarValue::DurationMicrosecond(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::DurationMicrosecond(None) => Ok(ast::Value::Null),
        ScalarValue::DurationNanosecond(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::DurationNanosecond(None) => Ok(ast::Value::Null),
        ScalarValue::Struct(Some(_), _) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Struct(None, _) => Ok(ast::Value::Null),
        ScalarValue::Dictionary(..) => not_impl_err!("Unsupported scalar: {v:?}"),
    }
}

fn col_to_sql(col: &Column) -> Result<ast::Expr> {
    Ok(ast::Expr::CompoundIdentifier(
        [
            col.relation.as_ref().unwrap().table().to_string(),
            col.name.to_string(),
        ]
        .iter()
        .map(|i| new_ident(i.to_string()))
        .collect(),
    ))
}

fn join_operator_to_sql(join_type: JoinType, constraint: ast::JoinConstraint) -> JoinOperator {
    match join_type {
        JoinType::Inner => JoinOperator::Inner(constraint),
        JoinType::Left => JoinOperator::LeftOuter(constraint),
        JoinType::Right => JoinOperator::RightOuter(constraint),
        JoinType::Full => JoinOperator::FullOuter(constraint),
        JoinType::LeftAnti => JoinOperator::LeftAnti(constraint),
        JoinType::LeftSemi => JoinOperator::LeftSemi(constraint),
        JoinType::RightAnti => JoinOperator::RightAnti(constraint),
        JoinType::RightSemi => JoinOperator::RightSemi(constraint),
    }
}

fn join_conditions_to_sql(
    join_conditions: &Vec<(Expr, Expr)>,
    eq_op: ast::BinaryOperator,
    left_schema: &DFSchemaRef,
    right_schema: &DFSchemaRef,
) -> Result<Option<SQLExpr>> {
    // Only support AND conjunction for each binary expression in join conditions
    let mut exprs: Vec<SQLExpr> = vec![];
    for (left, right) in join_conditions {
        // Parse left
        let l = expr_to_sql(left, left_schema, 0)?;
        // Parse right
        let r = expr_to_sql(
            right,
            right_schema,
            left_schema.fields().len(), // offset to return the correct index
        )?;
        // AND with existing expression
        exprs.push(binary_op_to_sql(l, r, eq_op.clone()));
    }
    let join_expr: Option<SQLExpr> = exprs.into_iter().reduce(and_op_to_sql);
    Ok(join_expr)
}

pub fn and_op_to_sql(lhs: SQLExpr, rhs: SQLExpr) -> SQLExpr {
    binary_op_to_sql(lhs, rhs, ast::BinaryOperator::And)
}

pub fn binary_op_to_sql(lhs: SQLExpr, rhs: SQLExpr, op: ast::BinaryOperator) -> SQLExpr {
    SQLExpr::BinaryOp {
        left: Box::new(lhs),
        op,
        right: Box::new(rhs),
    }
}

fn new_table_alias(alias: String) -> ast::TableAlias {
    ast::TableAlias {
        name: new_ident(alias),
        columns: Vec::new(),
    }
}

fn new_ident(str: String) -> ast::Ident {
    ast::Ident {
        value: str,
        quote_style: Some('`'),
    }
}

fn dml_to_sql(_plan: &LogicalPlan) -> Result<ast::Statement> {
    Err(DataFusionError::NotImplemented(
        "dml unsupported".to_string(),
    ))
}

fn builder_error_to_df(e: BuilderError) -> DataFusionError {
    DataFusionError::External(format!("{e}").into())
}

#[cfg(test)]
mod tests {
    use datafusion::{execution::context::SessionContext, test_util::TestTableFactory};

    use super::*;

    #[tokio::test]
    async fn test_select() {
        let mut state = SessionContext::new().state();
        state
            .table_factories_mut()
            .insert("MOCKTABLE".to_string(), Arc::new(TestTableFactory {}));
        let ctx = SessionContext::new_with_state(state);

        ctx.sql("CREATE EXTERNAL TABLE table_a (id integer, value string) STORED AS MOCKTABLE LOCATION 'mock://path';").await.unwrap();
        ctx.sql("CREATE EXTERNAL TABLE table_b (id integer, value string) STORED AS MOCKTABLE LOCATION 'mock://path';").await.unwrap();
        ctx.sql("CREATE EXTERNAL TABLE table_c (id integer, value string) STORED AS MOCKTABLE LOCATION 'mock://path';").await.unwrap();

        let tests: Vec<(&str, &str)> = vec![
            (
                "select ta.id from table_a ta;",
                r#"SELECT `ta`.`id` FROM `table_a` AS `ta`"#,
            ),
            (
                "select * from table_a limit 10;",
                r#"SELECT `table_a`.`id`, `table_a`.`value` FROM `table_a` LIMIT 10"#,
            ),
            (
                "select ta.id from table_a ta where ta.id > 1;",
                r#"SELECT `ta`.`id` FROM `table_a` AS `ta` WHERE `ta`.`id` > 1"#,
            ),
            (
                "select ta.id, tb.value from table_a ta join table_b tb on ta.id = tb.id;",
                r#"SELECT `ta`.`id`, `tb`.`value` FROM `table_a` AS `ta` JOIN `table_b` AS `tb` ON `ta`.`id` = `tb`.`id`"#,
            ),
            (
                "select ta.id, tb.value from table_a ta join table_b tb on ta.id = tb.id join table_c tc on ta.id = tc.id;",
                r#"SELECT `ta`.`id`, `tb`.`value` FROM `table_a` AS `ta` JOIN `table_b` AS `tb` ON `ta`.`id` = `tb`.`id` JOIN `table_c` AS `tc` ON `ta`.`id` = `tc`.`id`"#,
            ),
        ];

        for (query, expected) in tests {
            // let dialect = GenericDialect {};
            // let orig_ast = Parser::parse_sql(&dialect, query).unwrap();
            // println!("{}", orig_ast[0]);
            let plan = ctx.sql(query).await.unwrap().into_unoptimized_plan();
            // println!("{:?}", plan);

            let ast = query_to_sql(&plan);
            // println!("{:?}", ast);

            assert!(ast.is_ok());
            let actual = format!("{}", ast.unwrap());
            assert_eq!(actual, expected);
        }
    }
}
