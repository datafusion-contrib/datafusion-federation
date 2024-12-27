use core::fmt;
use std::{any::Any, collections::HashMap, sync::Arc, vec};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    common::{Column, RecursionUnnestOption, UnnestOptions},
    config::ConfigOptions,
    error::{DataFusionError, Result},
    execution::{context::SessionState, TaskContext},
    logical_expr::{
        self,
        expr::{
            AggregateFunction, Alias, Exists, InList, InSubquery, ScalarFunction, Sort, Unnest,
            WindowFunction,
        },
        Between, BinaryExpr, Case, Cast, Expr, Extension, GroupingSet, Like, LogicalPlan,
        LogicalPlanBuilder, Projection, Subquery, TryCast,
    },
    optimizer::analyzer::{Analyzer, AnalyzerRule},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
    },
    sql::{
        unparser::{plan_to_sql, Unparser},
        TableReference,
    },
};
use datafusion_federation::{
    get_table_source, schema_cast, FederatedPlanNode, FederationPlanner, FederationProvider,
};

mod schema;
pub use schema::*;

mod executor;
pub use executor::*;

// #[macro_use]
// extern crate derive_builder;

// SQLFederationProvider provides federation to SQL DMBSs.
pub struct SQLFederationProvider {
    analyzer: Arc<Analyzer>,
    executor: Arc<dyn SQLExecutor>,
}

impl SQLFederationProvider {
    pub fn new(executor: Arc<dyn SQLExecutor>) -> Self {
        Self {
            analyzer: Arc::new(Analyzer::with_rules(vec![Arc::new(
                SQLFederationAnalyzerRule::new(Arc::clone(&executor)),
            )])),
            executor,
        }
    }
}

impl FederationProvider for SQLFederationProvider {
    fn name(&self) -> &str {
        "sql_federation_provider"
    }

    fn compute_context(&self) -> Option<String> {
        self.executor.compute_context()
    }

    fn analyzer(&self) -> Option<Arc<Analyzer>> {
        Some(Arc::clone(&self.analyzer))
    }
}

struct SQLFederationAnalyzerRule {
    planner: Arc<dyn FederationPlanner>,
}

impl std::fmt::Debug for SQLFederationAnalyzerRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SQLFederationAnalyzerRule").finish()
    }
}

impl SQLFederationAnalyzerRule {
    pub fn new(executor: Arc<dyn SQLExecutor>) -> Self {
        Self {
            planner: Arc::new(SQLFederationPlanner::new(Arc::clone(&executor))),
        }
    }
}

impl AnalyzerRule for SQLFederationAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let fed_plan = FederatedPlanNode::new(plan.clone(), Arc::clone(&self.planner));
        let ext_node = Extension {
            node: Arc::new(fed_plan),
        };
        Ok(LogicalPlan::Extension(ext_node))
    }

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str {
        "federate_sql"
    }
}

/// Rewrite table scans to use the original federated table name.
fn rewrite_table_scans(
    plan: &LogicalPlan,
    known_rewrites: &mut HashMap<TableReference, TableReference>,
) -> Result<LogicalPlan> {
    if plan.inputs().is_empty() {
        if let LogicalPlan::TableScan(table_scan) = plan {
            let original_table_name = table_scan.table_name.clone();
            let mut new_table_scan = table_scan.clone();

            let Some(federated_source) = get_table_source(&table_scan.source)? else {
                // Not a federated source
                return Ok(plan.clone());
            };

            match federated_source.as_any().downcast_ref::<SQLTableSource>() {
                Some(sql_table_source) => {
                    let remote_table_name = TableReference::from(sql_table_source.table_name());
                    known_rewrites.insert(original_table_name, remote_table_name.clone());

                    // Rewrite the schema of this node to have the remote table as the qualifier.
                    let new_schema = (*new_table_scan.projected_schema)
                        .clone()
                        .replace_qualifier(remote_table_name.clone());
                    new_table_scan.projected_schema = Arc::new(new_schema);
                    new_table_scan.table_name = remote_table_name;
                }
                None => {
                    // Not a SQLTableSource (is this possible?)
                    return Ok(plan.clone());
                }
            }

            return Ok(LogicalPlan::TableScan(new_table_scan));
        } else {
            return Ok(plan.clone());
        }
    }

    let rewritten_inputs = plan
        .inputs()
        .into_iter()
        .map(|i| rewrite_table_scans(i, known_rewrites))
        .collect::<Result<Vec<_>>>()?;

    match plan {
        LogicalPlan::Unnest(unnest) => {
            // The Union plan cannot be constructed from rewritten expressions. It requires specialized logic to handle
            // the renaming in UNNEST columns and the corresponding column aliases in the underlying projection plan.
            rewrite_unnest_plan(unnest, rewritten_inputs, known_rewrites)
        }
        _ => {
            let mut new_expressions = vec![];
            for expression in plan.expressions() {
                let new_expr = rewrite_table_scans_in_expr(expression.clone(), known_rewrites)?;
                new_expressions.push(new_expr);
            }
            let new_plan = plan.with_new_exprs(new_expressions, rewritten_inputs)?;
            Ok(new_plan)
        }
    }
}

/// Rewrite an unnest plan to use the original federated table name.
/// In a standard unnest plan, column names are typically referenced in projection columns by wrapping them
/// in aliases such as "UNNEST(table_name.column_name)". `rewrite_table_scans_in_expr` does not handle alias
/// rewriting so we manually collect the rewritten unnest column names/aliases and update the projection
/// plan to ensure that the aliases reflect the new names.
fn rewrite_unnest_plan(
    unnest: &logical_expr::Unnest,
    mut rewritten_inputs: Vec<LogicalPlan>,
    known_rewrites: &mut HashMap<TableReference, TableReference>,
) -> Result<LogicalPlan> {
    // Unnest plan has a single input
    let input = rewritten_inputs.remove(0);

    let mut known_unnest_rewrites: HashMap<String, String> = HashMap::new();

    // `exec_columns` represent columns to run UNNEST on: rewrite them and collect new names
    let unnest_columns = unnest
        .exec_columns
        .iter()
        .map(|c: &Column| {
            match rewrite_table_scans_in_expr(Expr::Column(c.clone()), known_rewrites)? {
                Expr::Column(column) => {
                    known_unnest_rewrites.insert(c.name.clone(), column.name.clone());
                    Ok(column)
                }
                _ => Err(DataFusionError::Plan(
                    "Rewritten column expression must be a column".to_string(),
                )),
            }
        })
        .collect::<Result<Vec<Column>>>()?;

    let LogicalPlan::Projection(projection) = input else {
        return Err(DataFusionError::Plan(
            "The input to the unnest plan should be a projection plan".to_string(),
        ));
    };

    // rewrite aliases in inner projection; columns were rewritten via `rewrite_table_scans_in_expr`
    let new_expressions = projection
        .expr
        .into_iter()
        .map(|expr| match expr {
            Expr::Alias(alias) => {
                let name = match known_unnest_rewrites.get(&alias.name) {
                    Some(name) => name,
                    None => &alias.name,
                };
                Ok(Expr::Alias(Alias::new(*alias.expr, alias.relation, name)))
            }
            _ => Ok(expr),
        })
        .collect::<Result<Vec<_>>>()?;

    let updated_unnest_inner_projection =
        Projection::try_new(new_expressions, Arc::clone(&projection.input))?;

    let unnest_options = rewrite_unnest_options(&unnest.options, known_rewrites);

    // reconstruct the unnest plan with updated projection and rewritten column names
    let new_plan =
        LogicalPlanBuilder::new(LogicalPlan::Projection(updated_unnest_inner_projection))
            .unnest_columns_with_options(unnest_columns, unnest_options)?
            .build()?;

    Ok(new_plan)
}

/// Rewrites columns names in the unnest options to use the original federated table name:
/// "unnest_placeholder(foo.df_table.a,depth=1)"" -> "unnest_placeholder(remote_table.a,depth=1)""
fn rewrite_unnest_options(
    options: &UnnestOptions,
    known_rewrites: &HashMap<TableReference, TableReference>,
) -> UnnestOptions {
    let mut new_options = options.clone();
    new_options
        .recursions
        .iter_mut()
        .for_each(|x: &mut RecursionUnnestOption| {
            if let Some(new_name) = rewrite_column_name(&x.input_column.name, known_rewrites) {
                x.input_column.name = new_name;
            }

            if let Some(new_name) = rewrite_column_name(&x.output_column.name, known_rewrites) {
                x.output_column.name = new_name;
            }
        });
    new_options
}

/// Checks if any of the rewrites match any substring in col_name, and replace that part of the string if so.
/// This handles cases like "MAX(foo.df_table.a)" -> "MAX(remote_table.a)"
/// Returns the rewritten name if any rewrite was applied, otherwise None.
fn rewrite_column_name(
    col_name: &str,
    known_rewrites: &HashMap<TableReference, TableReference>,
) -> Option<String> {
    let (new_col_name, was_rewritten) = known_rewrites.iter().fold(
        (col_name.to_string(), false),
        |(col_name, was_rewritten), (table_ref, rewrite)| match rewrite_column_name_in_expr(
            &col_name,
            &table_ref.to_string(),
            &rewrite.to_string(),
            0,
        ) {
            Some(new_name) => (new_name, true),
            None => (col_name, was_rewritten),
        },
    );

    if was_rewritten {
        Some(new_col_name)
    } else {
        None
    }
}

// The function replaces occurrences of table_ref_str in col_name with the new name defined by rewrite.
// The name to rewrite should NOT be a substring of another name.
// Supports multiple occurrences of table_ref_str in col_name.
fn rewrite_column_name_in_expr(
    col_name: &str,
    table_ref_str: &str,
    rewrite: &str,
    start_pos: usize,
) -> Option<String> {
    if start_pos >= col_name.len() {
        return None;
    }

    // Find the first occurrence of table_ref_str starting from start_pos
    let idx = col_name[start_pos..].find(table_ref_str)?;

    // Calculate the absolute index of the occurrence in string as the index above is relative to start_pos
    let idx = start_pos + idx;

    // Table name same as column name
    // Shouldn't rewrite in this case
    if idx == 0 && start_pos == 0 {
        return None;
    }

    if idx > 0 {
        // Check if the previous character is alphabetic, numeric, underscore or period, in which case we
        // should not rewrite as it is a part of another name.
        if let Some(prev_char) = col_name.chars().nth(idx - 1) {
            if prev_char.is_alphabetic()
                || prev_char.is_numeric()
                || prev_char == '_'
                || prev_char == '.'
            {
                return rewrite_column_name_in_expr(
                    col_name,
                    table_ref_str,
                    rewrite,
                    idx + table_ref_str.len(),
                );
            }
        }
    }

    // Check if the next character is alphabetic, numeric or underscore, in which case we
    // should not rewrite as it is a part of another name.
    if let Some(next_char) = col_name.chars().nth(idx + table_ref_str.len()) {
        if next_char.is_alphabetic() || next_char.is_numeric() || next_char == '_' {
            return rewrite_column_name_in_expr(
                col_name,
                table_ref_str,
                rewrite,
                idx + table_ref_str.len(),
            );
        }
    }

    // Found full match, replace table_ref_str occurrence with rewrite
    let rewritten_name = format!(
        "{}{}{}",
        &col_name[..idx],
        rewrite,
        &col_name[idx + table_ref_str.len()..]
    );

    // Check if the rewritten name contains more occurrence of table_ref_str, and rewrite them as well
    // This is done by providing the updated start_pos for search
    match rewrite_column_name_in_expr(&rewritten_name, table_ref_str, rewrite, idx + rewrite.len())
    {
        Some(new_name) => Some(new_name), // more occurrences found
        None => Some(rewritten_name),     // no more occurrences/changes
    }
}

fn rewrite_table_scans_in_expr(
    expr: Expr,
    known_rewrites: &mut HashMap<TableReference, TableReference>,
) -> Result<Expr> {
    match expr {
        Expr::ScalarSubquery(subquery) => {
            let new_subquery = rewrite_table_scans(&subquery.subquery, known_rewrites)?;
            let outer_ref_columns = subquery
                .outer_ref_columns
                .into_iter()
                .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                .collect::<Result<Vec<Expr>>>()?;
            Ok(Expr::ScalarSubquery(Subquery {
                subquery: Arc::new(new_subquery),
                outer_ref_columns,
            }))
        }
        Expr::BinaryExpr(binary_expr) => {
            let left = rewrite_table_scans_in_expr(*binary_expr.left, known_rewrites)?;
            let right = rewrite_table_scans_in_expr(*binary_expr.right, known_rewrites)?;
            Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left),
                binary_expr.op,
                Box::new(right),
            )))
        }
        Expr::Column(mut col) => {
            if let Some(rewrite) = col.relation.as_ref().and_then(|r| known_rewrites.get(r)) {
                Ok(Expr::Column(Column::new(Some(rewrite.clone()), &col.name)))
            } else {
                // This prevent over-eager rewrite and only pass the column into below rewritten
                // rule like MAX(...)
                if col.relation.is_some() {
                    return Ok(Expr::Column(col));
                }

                // Check if any of the rewrites match any substring in col.name, and replace that part of the string if so.
                // This will handles cases like "MAX(foo.df_table.a)" -> "MAX(remote_table.a)"
                if let Some(new_name) = rewrite_column_name(&col.name, known_rewrites) {
                    Ok(Expr::Column(Column::new(col.relation.take(), new_name)))
                } else {
                    Ok(Expr::Column(col))
                }
            }
        }
        Expr::Alias(alias) => {
            let expr = rewrite_table_scans_in_expr(*alias.expr, known_rewrites)?;
            if let Some(relation) = &alias.relation {
                if let Some(rewrite) = known_rewrites.get(relation) {
                    return Ok(Expr::Alias(Alias::new(
                        expr,
                        Some(rewrite.clone()),
                        alias.name,
                    )));
                }
            }
            Ok(Expr::Alias(Alias::new(expr, alias.relation, alias.name)))
        }
        Expr::Like(like) => {
            let expr = rewrite_table_scans_in_expr(*like.expr, known_rewrites)?;
            let pattern = rewrite_table_scans_in_expr(*like.pattern, known_rewrites)?;
            Ok(Expr::Like(Like::new(
                like.negated,
                Box::new(expr),
                Box::new(pattern),
                like.escape_char,
                like.case_insensitive,
            )))
        }
        Expr::SimilarTo(similar_to) => {
            let expr = rewrite_table_scans_in_expr(*similar_to.expr, known_rewrites)?;
            let pattern = rewrite_table_scans_in_expr(*similar_to.pattern, known_rewrites)?;
            Ok(Expr::SimilarTo(Like::new(
                similar_to.negated,
                Box::new(expr),
                Box::new(pattern),
                similar_to.escape_char,
                similar_to.case_insensitive,
            )))
        }
        Expr::Not(e) => {
            let expr = rewrite_table_scans_in_expr(*e, known_rewrites)?;
            Ok(Expr::Not(Box::new(expr)))
        }
        Expr::IsNotNull(e) => {
            let expr = rewrite_table_scans_in_expr(*e, known_rewrites)?;
            Ok(Expr::IsNotNull(Box::new(expr)))
        }
        Expr::IsNull(e) => {
            let expr = rewrite_table_scans_in_expr(*e, known_rewrites)?;
            Ok(Expr::IsNull(Box::new(expr)))
        }
        Expr::IsTrue(e) => {
            let expr = rewrite_table_scans_in_expr(*e, known_rewrites)?;
            Ok(Expr::IsTrue(Box::new(expr)))
        }
        Expr::IsFalse(e) => {
            let expr = rewrite_table_scans_in_expr(*e, known_rewrites)?;
            Ok(Expr::IsFalse(Box::new(expr)))
        }
        Expr::IsUnknown(e) => {
            let expr = rewrite_table_scans_in_expr(*e, known_rewrites)?;
            Ok(Expr::IsUnknown(Box::new(expr)))
        }
        Expr::IsNotTrue(e) => {
            let expr = rewrite_table_scans_in_expr(*e, known_rewrites)?;
            Ok(Expr::IsNotTrue(Box::new(expr)))
        }
        Expr::IsNotFalse(e) => {
            let expr = rewrite_table_scans_in_expr(*e, known_rewrites)?;
            Ok(Expr::IsNotFalse(Box::new(expr)))
        }
        Expr::IsNotUnknown(e) => {
            let expr = rewrite_table_scans_in_expr(*e, known_rewrites)?;
            Ok(Expr::IsNotUnknown(Box::new(expr)))
        }
        Expr::Negative(e) => {
            let expr = rewrite_table_scans_in_expr(*e, known_rewrites)?;
            Ok(Expr::Negative(Box::new(expr)))
        }
        Expr::Between(between) => {
            let expr = rewrite_table_scans_in_expr(*between.expr, known_rewrites)?;
            let low = rewrite_table_scans_in_expr(*between.low, known_rewrites)?;
            let high = rewrite_table_scans_in_expr(*between.high, known_rewrites)?;
            Ok(Expr::Between(Between::new(
                Box::new(expr),
                between.negated,
                Box::new(low),
                Box::new(high),
            )))
        }
        Expr::Case(case) => {
            let expr = case
                .expr
                .map(|e| rewrite_table_scans_in_expr(*e, known_rewrites))
                .transpose()?
                .map(Box::new);
            let else_expr = case
                .else_expr
                .map(|e| rewrite_table_scans_in_expr(*e, known_rewrites))
                .transpose()?
                .map(Box::new);
            let when_expr = case
                .when_then_expr
                .into_iter()
                .map(|(when, then)| {
                    let when = rewrite_table_scans_in_expr(*when, known_rewrites);
                    let then = rewrite_table_scans_in_expr(*then, known_rewrites);

                    match (when, then) {
                        (Ok(when), Ok(then)) => Ok((Box::new(when), Box::new(then))),
                        (Err(e), _) | (_, Err(e)) => Err(e),
                    }
                })
                .collect::<Result<Vec<(Box<Expr>, Box<Expr>)>>>()?;
            Ok(Expr::Case(Case::new(expr, when_expr, else_expr)))
        }
        Expr::Cast(cast) => {
            let expr = rewrite_table_scans_in_expr(*cast.expr, known_rewrites)?;
            Ok(Expr::Cast(Cast::new(Box::new(expr), cast.data_type)))
        }
        Expr::TryCast(try_cast) => {
            let expr = rewrite_table_scans_in_expr(*try_cast.expr, known_rewrites)?;
            Ok(Expr::TryCast(TryCast::new(
                Box::new(expr),
                try_cast.data_type,
            )))
        }
        Expr::ScalarFunction(sf) => {
            let args = sf
                .args
                .into_iter()
                .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                .collect::<Result<Vec<Expr>>>()?;
            Ok(Expr::ScalarFunction(ScalarFunction {
                func: sf.func,
                args,
            }))
        }
        Expr::AggregateFunction(af) => {
            let args = af
                .args
                .into_iter()
                .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                .collect::<Result<Vec<Expr>>>()?;
            let filter = af
                .filter
                .map(|e| rewrite_table_scans_in_expr(*e, known_rewrites))
                .transpose()?
                .map(Box::new);
            let order_by = af
                .order_by
                .map(|e| {
                    e.into_iter()
                        .map(|s| {
                            rewrite_table_scans_in_expr(s.expr, known_rewrites)
                                .map(|e| Sort::new(e, s.asc, s.nulls_first))
                        })
                        .collect::<Result<Vec<Sort>>>()
                })
                .transpose()?;
            Ok(Expr::AggregateFunction(AggregateFunction {
                func: af.func,
                args,
                distinct: af.distinct,
                filter,
                order_by,
                null_treatment: af.null_treatment,
            }))
        }
        Expr::WindowFunction(wf) => {
            let args = wf
                .args
                .into_iter()
                .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                .collect::<Result<Vec<Expr>>>()?;
            let partition_by = wf
                .partition_by
                .into_iter()
                .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                .collect::<Result<Vec<Expr>>>()?;
            let order_by = wf
                .order_by
                .into_iter()
                .map(|s| {
                    rewrite_table_scans_in_expr(s.expr, known_rewrites)
                        .map(|e| Sort::new(e, s.asc, s.nulls_first))
                })
                .collect::<Result<Vec<Sort>>>()?;
            Ok(Expr::WindowFunction(WindowFunction {
                fun: wf.fun,
                args,
                partition_by,
                order_by,
                window_frame: wf.window_frame,
                null_treatment: wf.null_treatment,
            }))
        }
        Expr::InList(il) => {
            let expr = rewrite_table_scans_in_expr(*il.expr, known_rewrites)?;
            let list = il
                .list
                .into_iter()
                .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                .collect::<Result<Vec<Expr>>>()?;
            Ok(Expr::InList(InList::new(Box::new(expr), list, il.negated)))
        }
        Expr::Exists(exists) => {
            let subquery_plan = rewrite_table_scans(&exists.subquery.subquery, known_rewrites)?;
            let outer_ref_columns = exists
                .subquery
                .outer_ref_columns
                .into_iter()
                .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                .collect::<Result<Vec<Expr>>>()?;
            let subquery = Subquery {
                subquery: Arc::new(subquery_plan),
                outer_ref_columns,
            };
            Ok(Expr::Exists(Exists::new(subquery, exists.negated)))
        }
        Expr::InSubquery(is) => {
            let expr = rewrite_table_scans_in_expr(*is.expr, known_rewrites)?;
            let subquery_plan = rewrite_table_scans(&is.subquery.subquery, known_rewrites)?;
            let outer_ref_columns = is
                .subquery
                .outer_ref_columns
                .into_iter()
                .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                .collect::<Result<Vec<Expr>>>()?;
            let subquery = Subquery {
                subquery: Arc::new(subquery_plan),
                outer_ref_columns,
            };
            Ok(Expr::InSubquery(InSubquery::new(
                Box::new(expr),
                subquery,
                is.negated,
            )))
        }
        Expr::Wildcard { qualifier, options } => {
            if let Some(rewrite) = qualifier.as_ref().and_then(|q| known_rewrites.get(q)) {
                Ok(Expr::Wildcard {
                    qualifier: Some(rewrite.clone()),
                    options,
                })
            } else {
                Ok(Expr::Wildcard { qualifier, options })
            }
        }
        Expr::GroupingSet(gs) => match gs {
            GroupingSet::Rollup(exprs) => {
                let exprs = exprs
                    .into_iter()
                    .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                    .collect::<Result<Vec<Expr>>>()?;
                Ok(Expr::GroupingSet(GroupingSet::Rollup(exprs)))
            }
            GroupingSet::Cube(exprs) => {
                let exprs = exprs
                    .into_iter()
                    .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                    .collect::<Result<Vec<Expr>>>()?;
                Ok(Expr::GroupingSet(GroupingSet::Cube(exprs)))
            }
            GroupingSet::GroupingSets(vec_exprs) => {
                let vec_exprs = vec_exprs
                    .into_iter()
                    .map(|exprs| {
                        exprs
                            .into_iter()
                            .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                            .collect::<Result<Vec<Expr>>>()
                    })
                    .collect::<Result<Vec<Vec<Expr>>>>()?;
                Ok(Expr::GroupingSet(GroupingSet::GroupingSets(vec_exprs)))
            }
        },
        Expr::OuterReferenceColumn(dt, col) => {
            if let Some(rewrite) = col.relation.as_ref().and_then(|r| known_rewrites.get(r)) {
                Ok(Expr::OuterReferenceColumn(
                    dt,
                    Column::new(Some(rewrite.clone()), &col.name),
                ))
            } else {
                Ok(Expr::OuterReferenceColumn(dt, col))
            }
        }
        Expr::Unnest(unnest) => {
            let expr = rewrite_table_scans_in_expr(*unnest.expr, known_rewrites)?;
            Ok(Expr::Unnest(Unnest::new(expr)))
        }
        Expr::ScalarVariable(_, _) | Expr::Literal(_) | Expr::Placeholder(_) => Ok(expr),
    }
}

struct SQLFederationPlanner {
    executor: Arc<dyn SQLExecutor>,
}

impl SQLFederationPlanner {
    pub fn new(executor: Arc<dyn SQLExecutor>) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl FederationPlanner for SQLFederationPlanner {
    async fn plan_federation(
        &self,
        node: &FederatedPlanNode,
        _session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(node.plan().schema().as_arrow().clone());
        let input = Arc::new(VirtualExecutionPlan::new(
            node.plan().clone(),
            Arc::clone(&self.executor),
        ));
        let schema_cast_exec = schema_cast::SchemaCastScanExec::new(input, schema);
        Ok(Arc::new(schema_cast_exec))
    }
}

#[derive(Debug, Clone)]
struct VirtualExecutionPlan {
    plan: LogicalPlan,
    executor: Arc<dyn SQLExecutor>,
    props: PlanProperties,
}

impl VirtualExecutionPlan {
    pub fn new(plan: LogicalPlan, executor: Arc<dyn SQLExecutor>) -> Self {
        let schema: Schema = plan.schema().as_ref().into();
        let props = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(schema)),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            plan,
            executor,
            props,
        }
    }

    fn schema(&self) -> SchemaRef {
        let df_schema = self.plan.schema().as_ref();
        Arc::new(Schema::from(df_schema))
    }

    fn sql(&self) -> Result<String> {
        // Find all table scans, recover the SQLTableSource, find the remote table name and replace the name of the TableScan table.
        let mut known_rewrites = HashMap::new();
        let mut ast = Unparser::new(self.executor.dialect().as_ref())
            .plan_to_sql(&rewrite_table_scans(&self.plan, &mut known_rewrites)?)?;

        if let Some(analyzer) = self.executor.ast_analyzer() {
            ast = analyzer(ast)?;
        }

        Ok(format!("{ast}"))
    }
}

impl DisplayAs for VirtualExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "VirtualExecutionPlan")?;
        let Ok(ast) = plan_to_sql(&self.plan) else {
            return Ok(());
        };
        write!(f, " name={}", self.executor.name())?;
        if let Some(ctx) = self.executor.compute_context() {
            write!(f, " compute_context={ctx}")?;
        };

        write!(f, " sql={ast}")?;
        if let Ok(query) = self.sql() {
            write!(f, " rewritten_sql={query}")?;
        };

        Ok(())
    }
}

impl ExecutionPlan for VirtualExecutionPlan {
    fn name(&self) -> &str {
        "VirtualExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.executor.execute(self.sql()?.as_str(), self.schema())
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field},
        catalog::SchemaProvider,
        catalog_common::MemorySchemaProvider,
        common::Column,
        datasource::{DefaultTableSource, TableProvider},
        error::DataFusionError,
        execution::context::SessionContext,
        logical_expr::LogicalPlanBuilder,
        sql::{unparser::dialect::DefaultDialect, unparser::dialect::Dialect},
    };
    use datafusion_federation::FederatedTableProviderAdaptor;

    use super::*;

    struct TestSQLExecutor {}

    #[async_trait]
    impl SQLExecutor for TestSQLExecutor {
        fn name(&self) -> &str {
            "test_sql_table_source"
        }

        fn compute_context(&self) -> Option<String> {
            None
        }

        fn dialect(&self) -> Arc<dyn Dialect> {
            Arc::new(DefaultDialect {})
        }

        fn execute(&self, _query: &str, _schema: SchemaRef) -> Result<SendableRecordBatchStream> {
            Err(DataFusionError::NotImplemented(
                "execute not implemented".to_string(),
            ))
        }

        async fn table_names(&self) -> Result<Vec<String>> {
            Err(DataFusionError::NotImplemented(
                "table inference not implemented".to_string(),
            ))
        }

        async fn get_table_schema(&self, _table_name: &str) -> Result<SchemaRef> {
            Err(DataFusionError::NotImplemented(
                "table inference not implemented".to_string(),
            ))
        }
    }

    fn get_test_table_provider() -> Arc<dyn TableProvider> {
        let sql_federation_provider =
            Arc::new(SQLFederationProvider::new(Arc::new(TestSQLExecutor {})));

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Date32, false),
            Field::new(
                "d",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
        ]));
        let table_source = Arc::new(
            SQLTableSource::new_with_schema(
                sql_federation_provider,
                "remote_table".to_string(),
                schema,
            )
            .expect("to have a valid SQLTableSource"),
        );
        Arc::new(FederatedTableProviderAdaptor::new(table_source))
    }

    fn get_test_table_source() -> Arc<DefaultTableSource> {
        Arc::new(DefaultTableSource::new(get_test_table_provider()))
    }

    fn get_test_df_context() -> SessionContext {
        let ctx = SessionContext::new();
        let catalog = ctx
            .catalog("datafusion")
            .expect("default catalog is datafusion");
        let foo_schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
        catalog
            .register_schema("foo", Arc::clone(&foo_schema))
            .expect("to register schema");
        foo_schema
            .register_table("df_table".to_string(), get_test_table_provider())
            .expect("to register table");

        let public_schema = catalog
            .schema("public")
            .expect("public schema should exist");
        public_schema
            .register_table("app_table".to_string(), get_test_table_provider())
            .expect("to register table");

        ctx
    }

    #[test]
    fn test_rewrite_table_scans_basic() -> Result<()> {
        let default_table_source = get_test_table_source();
        let plan =
            LogicalPlanBuilder::scan("foo.df_table", default_table_source, None)?.project(vec![
                Expr::Column(Column::from_qualified_name("foo.df_table.a")),
                Expr::Column(Column::from_qualified_name("foo.df_table.b")),
                Expr::Column(Column::from_qualified_name("foo.df_table.c")),
            ])?;

        let mut known_rewrites = HashMap::new();
        let rewritten_plan = rewrite_table_scans(&plan.build()?, &mut known_rewrites)?;

        println!("rewritten_plan: \n{:#?}", rewritten_plan);

        let unparsed_sql = plan_to_sql(&rewritten_plan)?;

        println!("unparsed_sql: \n{unparsed_sql}");

        assert_eq!(
            format!("{unparsed_sql}"),
            r#"SELECT remote_table.a, remote_table.b, remote_table.c FROM remote_table"#
        );

        Ok(())
    }

    fn init_tracing() {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter("debug")
            .with_ansi(true)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    }

    #[tokio::test]
    async fn test_rewrite_table_scans_agg() -> Result<()> {
        init_tracing();
        let ctx = get_test_df_context();

        let agg_tests = vec![
            (
                "SELECT MAX(a) FROM foo.df_table",
                r#"SELECT max(remote_table.a) FROM remote_table"#,
            ),
            (
                "SELECT foo.df_table.a FROM foo.df_table",
                r#"SELECT remote_table.a FROM remote_table"#,
            ),
            (
                "SELECT MIN(a) FROM foo.df_table",
                r#"SELECT min(remote_table.a) FROM remote_table"#,
            ),
            (
                "SELECT AVG(a) FROM foo.df_table",
                r#"SELECT avg(remote_table.a) FROM remote_table"#,
            ),
            (
                "SELECT SUM(a) FROM foo.df_table",
                r#"SELECT sum(remote_table.a) FROM remote_table"#,
            ),
            (
                "SELECT COUNT(a) FROM foo.df_table",
                r#"SELECT count(remote_table.a) FROM remote_table"#,
            ),
            (
                "SELECT COUNT(a) as cnt FROM foo.df_table",
                r#"SELECT count(remote_table.a) AS cnt FROM remote_table"#,
            ),
            (
                "SELECT COUNT(a) as cnt FROM foo.df_table",
                r#"SELECT count(remote_table.a) AS cnt FROM remote_table"#,
            ),
            (
                "SELECT app_table from (SELECT a as app_table FROM app_table) b",
                r#"SELECT b.app_table FROM (SELECT remote_table.a AS app_table FROM remote_table) AS b"#,
            ),
            (
                "SELECT MAX(app_table) from (SELECT a as app_table FROM app_table) b",
                r#"SELECT max(b.app_table) FROM (SELECT remote_table.a AS app_table FROM remote_table) AS b"#,
            ),
            // multiple occurrences of the same table in single aggregation expression
            (
                "SELECT COUNT(CASE WHEN a > 0 THEN a ELSE 0 END) FROM app_table",
                r#"SELECT count(CASE WHEN (remote_table.a > 0) THEN remote_table.a ELSE 0 END) FROM remote_table"#,
            ),
            // different tables in single aggregation expression
            (
                "SELECT COUNT(CASE WHEN appt.a > 0 THEN appt.a ELSE dft.a END) FROM app_table as appt, foo.df_table as dft",
                "SELECT count(CASE WHEN (appt.a > 0) THEN appt.a ELSE dft.a END) FROM remote_table AS appt JOIN remote_table AS dft"
            ),
        ];

        for test in agg_tests {
            test_sql(&ctx, test.0, test.1).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_rewrite_table_scans_alias() -> Result<()> {
        init_tracing();
        let ctx = get_test_df_context();

        let tests = vec![
            (
                "SELECT COUNT(app_table_a) FROM (SELECT a as app_table_a FROM app_table)",
                r#"SELECT count(app_table_a) FROM (SELECT remote_table.a AS app_table_a FROM remote_table)"#,
            ),
            (
                "SELECT app_table_a FROM (SELECT a as app_table_a FROM app_table)",
                r#"SELECT app_table_a FROM (SELECT remote_table.a AS app_table_a FROM remote_table)"#,
            ),
            (
                "SELECT aapp_table FROM (SELECT a as aapp_table FROM app_table)",
                r#"SELECT aapp_table FROM (SELECT remote_table.a AS aapp_table FROM remote_table)"#,
            ),
        ];

        for test in tests {
            test_sql(&ctx, test.0, test.1).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_rewrite_table_scans_unnest() -> Result<()> {
        init_tracing();
        let ctx = get_test_df_context();

        let tests = vec![
            (
                "SELECT UNNEST([1, 2, 2, 5, NULL]), b, c from app_table where a > 10 order by b limit 10;",
                r#"SELECT UNNEST(make_array(1, 2, 2, 5, NULL)) AS "UNNEST(make_array(Int64(1),Int64(2),Int64(2),Int64(5),NULL))", remote_table.b, remote_table.c FROM remote_table WHERE (remote_table.a > 10) ORDER BY remote_table.b ASC NULLS LAST LIMIT 10"#,
            ),
            (
                "SELECT UNNEST(app_table.d), b, c from app_table where a > 10 order by b limit 10;",
                r#"SELECT UNNEST(remote_table.d) AS "UNNEST(app_table.d)", remote_table.b, remote_table.c FROM remote_table WHERE (remote_table.a > 10) ORDER BY remote_table.b ASC NULLS LAST LIMIT 10"#,
            ),
            (
                "SELECT sum(b.x) AS total FROM (SELECT UNNEST(d) AS x from app_table where a > 0) AS b;",
                r#"SELECT sum(b.x) AS total FROM (SELECT UNNEST(remote_table.d) AS x FROM remote_table WHERE (remote_table.a > 0)) AS b"#,
            ),
        ];

        for test in tests {
            test_sql(&ctx, test.0, test.1).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_rewrite_same_column_table_name() -> Result<()> {
        init_tracing();
        let ctx = get_test_df_context();

        let tests = vec![(
            "SELECT app_table FROM (SELECT a app_table from app_table limit 100);",
            r#"SELECT app_table FROM (SELECT remote_table.a AS app_table FROM remote_table LIMIT 100)"#,
        )];

        for test in tests {
            test_sql(&ctx, test.0, test.1).await?;
        }

        Ok(())
    }

    async fn test_sql(
        ctx: &SessionContext,
        sql_query: &str,
        expected_sql: &str,
    ) -> Result<(), datafusion::error::DataFusionError> {
        let data_frame = ctx.sql(sql_query).await?;

        // println!("before optimization: \n{:#?}", data_frame.logical_plan());

        let mut known_rewrites = HashMap::new();
        let rewritten_plan = rewrite_table_scans(data_frame.logical_plan(), &mut known_rewrites)?;

        // println!("rewritten_plan: \n{:#?}", rewritten_plan);

        let unparsed_sql = plan_to_sql(&rewritten_plan)?;

        println!("unparsed_sql: \n{unparsed_sql}");

        assert_eq!(
            format!("{unparsed_sql}"),
            expected_sql,
            "SQL under test: {}",
            sql_query
        );

        Ok(())
    }
}
