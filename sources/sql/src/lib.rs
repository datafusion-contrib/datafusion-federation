use core::fmt;
use std::{any::Any, collections::HashMap, sync::Arc, vec};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    common::Column,
    config::ConfigOptions,
    error::Result,
    execution::{context::SessionState, TaskContext},
    logical_expr::{
        expr::{
            AggregateFunction, Alias, Exists, InList, InSubquery, ScalarFunction, Sort, Unnest,
            WindowFunction,
        },
        Between, BinaryExpr, Case, Cast, Expr, Extension, GetIndexedField, GroupingSet, Like,
        LogicalPlan, Subquery, TryCast,
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

#[cfg(feature = "connectorx")]
pub mod connectorx;
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

impl SQLFederationAnalyzerRule {
    pub fn new(executor: Arc<dyn SQLExecutor>) -> Self {
        Self {
            planner: Arc::new(SQLFederationPlanner::new(Arc::clone(&executor))),
        }
    }
}

impl AnalyzerRule for SQLFederationAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // Find all table scans, recover the SQLTableSource, find the remote table name and replace the name of the TableScan table.
        let mut known_rewrites = HashMap::new();
        let plan = rewrite_table_scans(&plan, &mut known_rewrites)?;

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

    let mut new_expressions = vec![];
    for expression in plan.expressions() {
        let new_expr = rewrite_table_scans_in_expr(expression.clone(), known_rewrites)?;
        new_expressions.push(new_expr);
    }

    let new_plan = plan.with_new_exprs(new_expressions, rewritten_inputs)?;

    Ok(new_plan)
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
                // Check if any of the rewrites match any substring in col.name, and replace that part of the string if so.
                // This will handles cases like "MAX(foo.df_table.a)" -> "MAX(remote_table.a)"
                let rewritten_name = known_rewrites.iter().find_map(|(table_ref, rewrite)| {
                    let table_ref_str = table_ref.to_string();
                    if col.name.contains(&table_ref_str) {
                        Some(col.name.replace(&table_ref_str, &rewrite.to_string()))
                    } else {
                        None
                    }
                });
                if let Some(new_name) = rewritten_name {
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
        Expr::GetIndexedField(indexed_field) => {
            let expr = rewrite_table_scans_in_expr(*indexed_field.expr, known_rewrites)?;
            Ok(Expr::GetIndexedField(GetIndexedField::new(
                Box::new(expr),
                indexed_field.field,
            )))
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
        Expr::Sort(sort) => {
            let expr = rewrite_table_scans_in_expr(*sort.expr, known_rewrites)?;
            Ok(Expr::Sort(Sort::new(
                Box::new(expr),
                sort.asc,
                sort.nulls_first,
            )))
        }
        Expr::ScalarFunction(sf) => {
            let args = sf
                .args
                .into_iter()
                .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                .collect::<Result<Vec<Expr>>>()?;
            Ok(Expr::ScalarFunction(ScalarFunction {
                func_def: sf.func_def,
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
                        .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                        .collect::<Result<Vec<Expr>>>()
                })
                .transpose()?;
            Ok(Expr::AggregateFunction(AggregateFunction {
                func_def: af.func_def,
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
                .map(|e| rewrite_table_scans_in_expr(e, known_rewrites))
                .collect::<Result<Vec<Expr>>>()?;
            Ok(Expr::WindowFunction(WindowFunction::new(
                wf.fun,
                args,
                partition_by,
                order_by,
                wf.window_frame,
                wf.null_treatment,
            )))
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
        Expr::Wildcard { qualifier } => {
            if let Some(rewrite) = qualifier
                .as_ref()
                .and_then(|q| known_rewrites.get(&TableReference::from(q)))
            {
                Ok(Expr::Wildcard {
                    qualifier: Some(rewrite.clone().to_string()),
                })
            } else {
                Ok(Expr::Wildcard { qualifier })
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
        }
        write!(f, " sql={ast}")
    }
}

impl ExecutionPlan for VirtualExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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
        let dialect = self.executor.dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let ast = unparser.plan_to_sql(&self.plan)?;
        let query = format!("{ast}");

        self.executor.execute(query.as_str(), self.schema())
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field},
        catalog::schema::{MemorySchemaProvider, SchemaProvider},
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
            r#"SELECT "remote_table"."a", "remote_table"."b", "remote_table"."c" FROM "remote_table""#
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
                r#"SELECT MAX("remote_table"."a") FROM "remote_table""#,
            ),
            (
                "SELECT MIN(a) FROM foo.df_table",
                r#"SELECT MIN("remote_table"."a") FROM "remote_table""#,
            ),
            (
                "SELECT AVG(a) FROM foo.df_table",
                r#"SELECT AVG("remote_table"."a") FROM "remote_table""#,
            ),
            (
                "SELECT SUM(a) FROM foo.df_table",
                r#"SELECT SUM("remote_table"."a") FROM "remote_table""#,
            ),
            (
                "SELECT COUNT(a) FROM foo.df_table",
                r#"SELECT COUNT("remote_table"."a") FROM "remote_table""#,
            ),
            (
                "SELECT COUNT(a) as cnt FROM foo.df_table",
                r#"SELECT COUNT("remote_table"."a") AS "cnt" FROM "remote_table""#,
            ),
        ];

        for test in agg_tests {
            let data_frame = ctx.sql(test.0).await?;

            println!("before optimization: \n{:#?}", data_frame.logical_plan());

            let mut known_rewrites = HashMap::new();
            let rewritten_plan =
                rewrite_table_scans(data_frame.logical_plan(), &mut known_rewrites)?;

            println!("rewritten_plan: \n{:#?}", rewritten_plan);

            let unparsed_sql = plan_to_sql(&rewritten_plan)?;

            println!("unparsed_sql: \n{unparsed_sql}");

            assert_eq!(
                format!("{unparsed_sql}"),
                test.1,
                "SQL under test: {}",
                test.0
            );
        }

        Ok(())
    }
}
