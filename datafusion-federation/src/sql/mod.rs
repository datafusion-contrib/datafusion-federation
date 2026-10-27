mod analyzer;
pub mod ast_analyzer;
mod dynamic_filter;
mod executor;
mod schema;
mod table;
mod table_reference;

use std::{any::Any, fmt, sync::Arc, vec};

use analyzer::RewriteTableScanAnalyzer;
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    catalog::Session,
    common::{
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
        Statistics,
    },
    config::ConfigOptions,
    error::{DataFusionError, Result},
    execution::TaskContext,
    logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder},
    optimizer::{optimizer::Optimizer, OptimizerConfig, OptimizerRule},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        apply_expression_roots,
        execution_plan::{Boundedness, EmissionType},
        filter_pushdown::{ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation},
        metrics::MetricsSet,
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
        SendableRecordBatchStream, StatisticsArgs,
    },
    sql::{sqlparser::ast::Statement, unparser::Unparser},
};
use futures::TryStreamExt;

pub use executor::{AstAnalyzer, LogicalOptimizer, SQLExecutor, SQLExecutorRef, SqlQueryRewriter};
pub use schema::{MultiSchemaProvider, SQLSchemaProvider};
pub use table::{RemoteTable, SQLTable, SQLTableSource};
pub use table_reference::RemoteTableRef;

use crate::{
    get_table_source, schema_cast, FederatedPlanNode, FederationPlanner, FederationProvider,
};

// SQLFederationProvider provides federation to SQL DMBSs.
#[derive(Debug)]
pub struct SQLFederationProvider {
    pub optimizer: Arc<Optimizer>,
    pub executor: Arc<dyn SQLExecutor>,
}

impl SQLFederationProvider {
    pub fn new(executor: Arc<dyn SQLExecutor>) -> Self {
        Self {
            optimizer: Arc::new(Optimizer::with_rules(vec![Arc::new(
                SQLFederationOptimizerRule::new(executor.clone()),
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

    fn optimizer(&self) -> Option<Arc<Optimizer>> {
        Some(self.optimizer.clone())
    }
}

#[derive(Debug)]
struct SQLFederationOptimizerRule {
    planner: Arc<SQLFederationPlanner>,
}

impl SQLFederationOptimizerRule {
    pub fn new(executor: Arc<dyn SQLExecutor>) -> Self {
        Self {
            planner: Arc::new(SQLFederationPlanner::new(Arc::clone(&executor))),
        }
    }
}

impl OptimizerRule for SQLFederationOptimizerRule {
    /// Try to rewrite `plan` to an optimized form, returning `Transformed::yes`
    /// if the plan was rewritten and `Transformed::no` if it was not.
    ///
    /// Note: this function is only called if [`Self::supports_rewrite`] returns
    /// true. Otherwise the Optimizer calls  [`Self::try_optimize`]
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Extension(Extension { ref node }) = plan {
            if node.name() == "Federated" {
                // Avoid attempting double federation
                return Ok(Transformed::no(plan));
            }
        }

        let fed_plan = FederatedPlanNode::new(plan.clone(), self.planner.clone());
        let ext_node = Extension {
            node: Arc::new(fed_plan),
        };

        let mut plan = LogicalPlan::Extension(ext_node);
        if let Some(mut rewriter) = self.planner.executor.logical_optimizer() {
            plan = rewriter(plan)?;
        }

        Ok(Transformed::yes(plan))
    }

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str {
        "federate_sql"
    }

    /// Does this rule support rewriting owned plans (rather than by reference)?
    fn supports_rewrite(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct SQLFederationPlanner {
    pub executor: Arc<dyn SQLExecutor>,
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
        _session_state: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(node.plan().schema().as_arrow().clone());
        let plan = node.plan().clone();
        let statistics = self.executor.statistics(&plan).await?;
        let input = Arc::new(VirtualExecutionPlan::new(
            plan,
            Arc::clone(&self.executor),
            statistics,
        ));
        let schema_cast_exec = schema_cast::SchemaCastScanExec::new(input, schema);
        Ok(Arc::new(schema_cast_exec))
    }
}

/// The alias of the derived table the runtime/dynamic filters are applied to.
const FILTERED_PLAN_ALIAS: &str = "federated_filtered";

#[derive(Debug, Clone)]
pub struct VirtualExecutionPlan {
    plan: LogicalPlan,
    executor: Arc<dyn SQLExecutor>,
    props: Arc<PlanProperties>,
    statistics: Statistics,
    filters: Vec<Arc<dyn PhysicalExpr>>,
}

impl VirtualExecutionPlan {
    pub fn new(plan: LogicalPlan, executor: Arc<dyn SQLExecutor>, statistics: Statistics) -> Self {
        let schema: Schema = plan.schema().as_arrow().clone();
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            plan,
            executor,
            props,
            statistics,
            filters: Vec::new(),
        }
    }

    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }

    pub fn executor(&self) -> &Arc<dyn SQLExecutor> {
        &self.executor
    }

    pub fn statistics(&self) -> &Statistics {
        &self.statistics
    }

    fn schema(&self) -> SchemaRef {
        let df_schema = self.plan.schema().as_arrow().clone();
        Arc::new(df_schema)
    }

    fn final_sql(&self) -> Result<String> {
        self.plan_to_final_sql(self.plan.clone())
    }

    /// Like [`Self::final_sql`], but narrows down the query with the current state of the
    /// runtime filters, as far as they can be expressed in SQL.
    ///
    /// Falls back to the unfiltered query if the filtered plan cannot be unparsed.
    fn final_sql_with_filters(&self) -> Result<String> {
        if let Some(plan) = self.filtered_plan()? {
            if let Ok(sql) = self.plan_to_final_sql(plan) {
                return Ok(sql);
            }
        }
        self.final_sql()
    }

    /// Wraps the federated plan as a derived table filtered by the runtime filters, so the
    /// predicate applies to the output of the plan regardless of its shape (e.g. a `LIMIT`).
    fn filtered_plan(&self) -> Result<Option<LogicalPlan>> {
        if self.filters.is_empty() {
            return Ok(None);
        }
        let Ok(aliased) = LogicalPlanBuilder::from(self.plan.clone()).alias(FILTERED_PLAN_ALIAS)
        else {
            return Ok(None);
        };
        let Some(predicate) =
            dynamic_filter::filters_to_predicate(&self.filters, &self.schema(), aliased.schema())?
        else {
            return Ok(None);
        };
        aliased.filter(predicate)?.build().map(Some)
    }

    fn plan_to_final_sql(&self, plan: LogicalPlan) -> Result<String> {
        let plan = RewriteTableScanAnalyzer::rewrite(plan)?;
        let (logical_optimizers, ast_analyzers, sql_query_rewriters) = gather_analyzers(&plan)?;
        let plan = apply_logical_optimizers(plan, logical_optimizers)?;
        let ast = self.plan_to_statement(&plan)?;
        let ast = self.rewrite_with_executor_ast_analyzer(ast)?;
        let ast = apply_ast_analyzers(ast, ast_analyzers)?;
        apply_sql_query_rewriters(ast.to_string(), sql_query_rewriters)
    }

    fn rewrite_with_executor_ast_analyzer(
        &self,
        ast: Statement,
    ) -> Result<Statement, datafusion::error::DataFusionError> {
        if let Some(mut analyzer) = self.executor.ast_analyzer() {
            Ok(analyzer(ast)?)
        } else {
            Ok(ast)
        }
    }

    fn plan_to_statement(&self, plan: &LogicalPlan) -> Result<Statement> {
        Unparser::new(self.executor.dialect().as_ref()).plan_to_sql(plan)
    }
}

fn gather_analyzers(
    plan: &LogicalPlan,
) -> Result<(
    Vec<LogicalOptimizer>,
    Vec<AstAnalyzer>,
    Vec<SqlQueryRewriter>,
)> {
    let mut logical_optimizers = vec![];
    let mut ast_analyzers = vec![];
    let mut sql_query_rewriters = vec![];

    plan.apply(|node| {
        if let LogicalPlan::TableScan(table) = node {
            let provider = get_table_source(&table.source)
                .expect("caller is virtual exec so this is valid")
                .expect("caller is virtual exec so this is valid");
            if let Some(source) = (provider.as_ref() as &dyn Any).downcast_ref::<SQLTableSource>() {
                if let Some(analyzer) = source.table.logical_optimizer() {
                    logical_optimizers.push(analyzer);
                }
                if let Some(analyzer) = source.table.ast_analyzer() {
                    ast_analyzers.push(analyzer);
                }
                if let Some(rewriter) = source.table.sql_query_rewriter() {
                    sql_query_rewriters.push(rewriter);
                }
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })?;

    Ok((logical_optimizers, ast_analyzers, sql_query_rewriters))
}

fn apply_logical_optimizers(
    mut plan: LogicalPlan,
    analyzers: Vec<LogicalOptimizer>,
) -> Result<LogicalPlan> {
    for mut analyzer in analyzers {
        let old_schema = plan.schema().clone();
        plan = analyzer(plan)?;
        let new_schema = plan.schema();
        if &old_schema != new_schema {
            return Err(DataFusionError::Execution(format!(
                "Schema altered during logical analysis, expected: {}, found: {}",
                old_schema, new_schema
            )));
        }
    }
    Ok(plan)
}

fn apply_ast_analyzers(mut statement: Statement, analyzers: Vec<AstAnalyzer>) -> Result<Statement> {
    for mut analyzer in analyzers {
        statement = analyzer(statement)?;
    }
    Ok(statement)
}

fn apply_sql_query_rewriters(
    mut query: String,
    rewriters: Vec<SqlQueryRewriter>,
) -> Result<String> {
    for mut rewriter in rewriters {
        query = rewriter(query)?;
    }
    Ok(query)
}

impl DisplayAs for VirtualExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "VirtualExecutionPlan")?;
        write!(f, " name={}", self.executor.name())?;
        if let Some(ctx) = self.executor.compute_context() {
            write!(f, " compute_context={ctx}")?;
        };
        let mut plan = match RewriteTableScanAnalyzer::rewrite(self.plan.clone()) {
            Ok(plan) => plan,
            Err(_) => self.plan.clone(),
        };
        if let Ok(statement) = self.plan_to_statement(&plan) {
            write!(f, " base_sql={statement}")?;
        }

        let (logical_optimizers, ast_analyzers, sql_query_rewriters) = match gather_analyzers(&plan)
        {
            Ok(analyzers) => analyzers,
            Err(_) => return Ok(()),
        };

        let old_plan = plan.clone();

        plan = match apply_logical_optimizers(plan, logical_optimizers) {
            Ok(plan) => plan,
            _ => return Ok(()),
        };

        let statement = match self.plan_to_statement(&plan) {
            Ok(statement) => statement,
            _ => return Ok(()),
        };

        if plan != old_plan {
            write!(f, " rewritten_logical_sql={statement}")?;
        }

        let old_statement = statement.clone();
        let statement = match self.rewrite_with_executor_ast_analyzer(statement) {
            Ok(statement) => statement,
            _ => return Ok(()),
        };
        if old_statement != statement {
            write!(f, " rewritten_executor_sql={statement}")?;
        }

        let old_statement = statement.clone();
        let statement = match apply_ast_analyzers(statement, ast_analyzers) {
            Ok(statement) => statement,
            _ => return Ok(()),
        };
        if old_statement != statement {
            write!(f, " rewritten_ast_analyzer={statement}")?;
        }

        let sql = statement.to_string();
        let rewritten_sql = match apply_sql_query_rewriters(sql.clone(), sql_query_rewriters) {
            Ok(sql) => sql,
            _ => return Ok(()),
        };
        if sql != rewritten_sql {
            write!(f, " rewritten_sql_query={rewritten_sql}")?;
        }

        if !self.filters.is_empty() {
            let filters: Vec<_> = self.filters.iter().map(|f| f.to_string()).collect();
            write!(f, " filters=[{}]", filters.join(", "))?;
        }

        Ok(())
    }
}

impl ExecutionPlan for VirtualExecutionPlan {
    fn name(&self) -> &str {
        "sql_federation_exec"
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
        if self.filters.is_empty() {
            return self
                .executor
                .execute(&self.final_sql()?, self.schema(), &[]);
        }

        // Dynamic filters are only populated once execution is underway (e.g. after a join has
        // built its hash table), so the SQL query is generated when the stream is first polled.
        let this = self.clone();
        let stream = futures::stream::once(async move {
            let sql = this.final_sql_with_filters()?;
            this.executor.execute(&sql, this.schema(), &this.filters)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(self.statistics.clone()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.executor.metrics()
    }

    /// Takes the dynamic filters pushed down at runtime (e.g. by joins and TopK) to narrow down
    /// the SQL query, and passes them to the [`SQLExecutor`].
    ///
    /// The filters are never reported as pushed down: only the parts that can be expressed in SQL
    /// end up in the query. Static filters (those pushed down before the
    /// [`FilterPushdownPhase::Post`] phase, such as the predicate of a `FilterExec`) are not
    /// taken at all, so DataFusion keeps evaluating them.
    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if phase != FilterPushdownPhase::Post || child_pushdown_result.parent_filters.is_empty() {
            return Ok(FilterPushdownPropagation::all_unsupported(
                child_pushdown_result,
            ));
        }

        let mut node = self.clone();
        node.filters.extend(
            child_pushdown_result
                .parent_filters
                .iter()
                .map(|f| Arc::clone(&f.filter)),
        );

        Ok(
            FilterPushdownPropagation::all_unsupported(child_pushdown_result)
                .with_updated_node(Arc::new(node) as Arc<dyn ExecutionPlan>),
        )
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(&self.filters, f)
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::table::RemoteTable;
    use super::*;
    use crate::sql::{
        RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTable, SQLTableSource,
    };
    use crate::FederatedTableProviderAdaptor;
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{Schema, SchemaRef};
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::common::TableReference;
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::physical_optimizer::{filter_pushdown::FilterPushdown, PhysicalOptimizerRule};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::filter_pushdown::ChildFilterPushdownResult;
    use datafusion::sql::unparser::dialect::Dialect;
    use datafusion::sql::unparser::{self};
    use datafusion::{
        arrow::datatypes::{DataType, Field},
        datasource::TableProvider,
        execution::context::SessionContext,
    };

    #[derive(Debug, Clone)]
    struct TestExecutor {
        compute_context: String,
    }

    #[async_trait]
    impl SQLExecutor for TestExecutor {
        fn name(&self) -> &str {
            "TestExecutor"
        }

        fn compute_context(&self) -> Option<String> {
            Some(self.compute_context.clone())
        }

        fn dialect(&self) -> Arc<dyn Dialect> {
            Arc::new(unparser::dialect::DefaultDialect {})
        }

        fn execute(
            &self,
            _query: &str,
            _schema: SchemaRef,
            _filters: &[Arc<dyn PhysicalExpr>],
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }

        async fn table_names(&self) -> Result<Vec<String>> {
            unimplemented!()
        }

        async fn get_table_schema(&self, _table_name: &str) -> Result<SchemaRef> {
            unimplemented!()
        }
    }

    fn get_test_table_provider(name: String, executor: TestExecutor) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Date32, false),
        ]));
        let table_ref = RemoteTableRef::try_from(name).unwrap();
        let table = Arc::new(RemoteTable::new(table_ref, schema));
        let provider = Arc::new(SQLFederationProvider::new(Arc::new(executor)));
        let table_source = Arc::new(SQLTableSource { provider, table });
        Arc::new(FederatedTableProviderAdaptor::new(table_source))
    }

    fn get_test_table_provider_with_table(
        table: Arc<dyn SQLTable>,
        executor: TestExecutor,
    ) -> Arc<dyn TableProvider> {
        let provider = Arc::new(SQLFederationProvider::new(Arc::new(executor)));
        let table_source = Arc::new(SQLTableSource::new_with_table(provider, table));
        Arc::new(FederatedTableProviderAdaptor::new(table_source))
    }

    #[derive(Debug)]
    struct SqlRewriteTable {
        table: RemoteTable,
        rewrite_calls: Arc<AtomicUsize>,
        suffix: String,
    }

    impl SqlRewriteTable {
        fn new(
            table_ref: RemoteTableRef,
            schema: SchemaRef,
            rewrite_calls: Arc<AtomicUsize>,
            suffix: impl Into<String>,
        ) -> Self {
            Self {
                table: RemoteTable::new(table_ref, schema),
                rewrite_calls,
                suffix: suffix.into(),
            }
        }
    }

    impl SQLTable for SqlRewriteTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn table_reference(&self) -> TableReference {
            self.table.table_reference().clone()
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(self.table.schema())
        }

        fn sql_query_rewriter(&self) -> Option<SqlQueryRewriter> {
            let rewrite_calls = Arc::clone(&self.rewrite_calls);
            let suffix = self.suffix.clone();
            Some(Box::new(move |sql| {
                rewrite_calls.fetch_add(1, Ordering::SeqCst);
                Ok(format!("{sql} {suffix}"))
            }))
        }
    }

    #[tokio::test]
    async fn basic_sql_federation_test() -> Result<(), DataFusionError> {
        let test_executor_a = TestExecutor {
            compute_context: "a".into(),
        };

        let test_executor_b = TestExecutor {
            compute_context: "b".into(),
        };

        let table_a1_ref = "table_a1".to_string();
        let table_a1 = get_test_table_provider(table_a1_ref.clone(), test_executor_a.clone());

        let table_a2_ref = "table_a2".to_string();
        let table_a2 = get_test_table_provider(table_a2_ref.clone(), test_executor_a);

        let table_b1_ref = "table_b1(1)".to_string();
        let table_b1_df_ref = "table_local_b1".to_string();

        let table_b1 = get_test_table_provider(table_b1_ref.clone(), test_executor_b);

        // Create a new SessionState with the optimizer rule we created above
        let state = crate::default_session_state();
        let ctx = SessionContext::new_with_state(state);

        ctx.register_table(table_a1_ref.clone(), table_a1).unwrap();
        ctx.register_table(table_a2_ref.clone(), table_a2).unwrap();
        ctx.register_table(table_b1_df_ref.clone(), table_b1)
            .unwrap();

        let query = r#"
            SELECT * FROM table_a1
            UNION ALL
            SELECT * FROM table_a2
            UNION ALL
            SELECT * FROM table_local_b1;
        "#;

        let df = ctx.sql(query).await?;

        let logical_plan = df.into_optimized_plan()?;

        let mut table_a1_federated = false;
        let mut table_a2_federated = false;
        let mut table_b1_federated = false;

        let _ = logical_plan.apply(|node| {
            if let LogicalPlan::Extension(node) = node {
                if let Some(node) = node.node.as_any().downcast_ref::<FederatedPlanNode>() {
                    let _ = node.plan().apply(|node| {
                        if let LogicalPlan::TableScan(table) = node {
                            if table.table_name.table() == table_a1_ref {
                                table_a1_federated = true;
                            }
                            if table.table_name.table() == table_a2_ref {
                                table_a2_federated = true;
                            }
                            // assuming table name is rewritten via analyzer
                            if table.table_name.table() == table_b1_df_ref {
                                table_b1_federated = true;
                            }
                        }
                        Ok(TreeNodeRecursion::Continue)
                    });
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });

        assert!(table_a1_federated);
        assert!(table_a2_federated);
        assert!(table_b1_federated);

        let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;

        let mut final_queries = vec![];

        let _ = physical_plan.apply(|node| {
            if node.name() == "sql_federation_exec" {
                let node = (node.as_ref() as &dyn Any)
                    .downcast_ref::<VirtualExecutionPlan>()
                    .unwrap();

                final_queries.push(node.final_sql()?);
            }
            Ok(TreeNodeRecursion::Continue)
        });

        let expected = vec![
            "SELECT table_a1.a, table_a1.b, table_a1.c FROM table_a1",
            "SELECT table_a2.a, table_a2.b, table_a2.c FROM table_a2",
            "SELECT table_b1.a, table_b1.b, table_b1.c FROM table_b1(1) AS table_b1",
        ];

        assert_eq!(
            HashSet::<&str>::from_iter(final_queries.iter().map(|x| x.as_str())),
            HashSet::from_iter(expected)
        );

        Ok(())
    }

    #[tokio::test]
    async fn multi_reference_sql_federation_test() -> Result<(), DataFusionError> {
        let test_executor_a = TestExecutor {
            compute_context: "test".into(),
        };

        let lowercase_table_ref = "default.table".to_string();
        let lowercase_local_table_ref = "dftable".to_string();
        let lowercase_table =
            get_test_table_provider(lowercase_table_ref.clone(), test_executor_a.clone());

        let capitalized_table_ref = "default.Table(1)".to_string();
        let capitalized_local_table_ref = "dfview".to_string();
        let capitalized_table =
            get_test_table_provider(capitalized_table_ref.clone(), test_executor_a);

        // Create a new SessionState with the optimizer rule we created above
        let state = crate::default_session_state();
        let ctx = SessionContext::new_with_state(state);

        ctx.register_table(lowercase_local_table_ref.clone(), lowercase_table)
            .unwrap();
        ctx.register_table(capitalized_local_table_ref.clone(), capitalized_table)
            .unwrap();

        let query = r#"
                SELECT * FROM dftable
                UNION ALL
                SELECT * FROM dfview;
            "#;

        let df = ctx.sql(query).await?;

        let logical_plan = df.into_optimized_plan()?;

        let mut lowercase_table = false;
        let mut capitalized_table = false;

        let _ = logical_plan.apply(|node| {
            if let LogicalPlan::Extension(node) = node {
                if let Some(node) = node.node.as_any().downcast_ref::<FederatedPlanNode>() {
                    let _ = node.plan().apply(|node| {
                        if let LogicalPlan::TableScan(table) = node {
                            if table.table_name.table() == lowercase_local_table_ref {
                                lowercase_table = true;
                            }
                            if table.table_name.table() == capitalized_local_table_ref {
                                capitalized_table = true;
                            }
                        }
                        Ok(TreeNodeRecursion::Continue)
                    });
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });

        assert!(lowercase_table);
        assert!(capitalized_table);

        let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;

        let mut final_queries = vec![];

        let _ = physical_plan.apply(|node| {
            if node.name() == "sql_federation_exec" {
                let node = (node.as_ref() as &dyn Any)
                    .downcast_ref::<VirtualExecutionPlan>()
                    .unwrap();

                final_queries.push(node.final_sql()?);
            }
            Ok(TreeNodeRecursion::Continue)
        });

        let expected = vec![
            r#"SELECT "table".a, "table".b, "table".c FROM "default"."table" UNION ALL SELECT "Table".a, "Table".b, "Table".c FROM "default"."Table"(1) AS Table"#,
        ];

        assert_eq!(
            HashSet::<&str>::from_iter(final_queries.iter().map(|x| x.as_str())),
            HashSet::from_iter(expected)
        );

        Ok(())
    }

    /// EXPLAIN ANALYZE must not federate the Analyze wrapper — only the inner
    /// query should be federated. Otherwise the SQL Unparser fails because it
    /// cannot convert Analyze to SQL.
    #[tokio::test]
    async fn explain_analyze_not_federated() -> Result<(), DataFusionError> {
        let executor = TestExecutor {
            compute_context: "a".into(),
        };

        let table_ref = "test_table".to_string();
        let table = get_test_table_provider(table_ref.clone(), executor);

        let state = crate::default_session_state();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(table_ref, table).unwrap();

        let plan = ctx
            .sql("EXPLAIN ANALYZE SELECT * FROM test_table")
            .await?
            .into_optimized_plan()?;

        // The top-level node must be Analyze, not Federated.
        assert!(
            matches!(plan, LogicalPlan::Analyze(_)),
            "Expected Analyze at root, got: {}",
            plan.display_indent()
        );

        // The inner plan should contain a Federated extension node.
        let mut found_federated = false;
        plan.apply(|node| {
            if let LogicalPlan::Extension(ext) = node {
                if ext.node.name() == "Federated" {
                    found_federated = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(
            found_federated,
            "Expected a Federated node inside the Analyze plan"
        );

        // Physical planning should succeed (this is where it used to fail).
        let physical_plan = ctx.state().create_physical_plan(&plan).await?;
        assert_eq!(physical_plan.name(), "AnalyzeExec");

        Ok(())
    }

    #[tokio::test]
    async fn sql_query_rewriter_hook_invoked_and_rewrites_sql() -> Result<(), DataFusionError> {
        let executor = TestExecutor {
            compute_context: "rewrite".into(),
        };
        let rewrite_calls = Arc::new(AtomicUsize::new(0));
        let table_ref = "table_with_rewriter".to_string();
        let table = Arc::new(SqlRewriteTable::new(
            table_ref.clone().try_into().unwrap(),
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Utf8, false),
                Field::new("c", DataType::Date32, false),
            ])),
            Arc::clone(&rewrite_calls),
            "/* rewritten by sql_query_rewriter */",
        ));
        let table_provider = get_test_table_provider_with_table(table, executor);

        let state = crate::default_session_state();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(table_ref.clone(), table_provider)
            .unwrap();

        let query = format!("SELECT * FROM {table_ref}");
        let df = ctx.sql(&query).await?;
        let logical_plan = df.into_optimized_plan()?;
        let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;

        let mut final_queries = vec![];
        let _ = physical_plan.apply(|node| {
            if node.name() == "sql_federation_exec" {
                let node = (node.as_ref() as &dyn Any)
                    .downcast_ref::<VirtualExecutionPlan>()
                    .unwrap();
                final_queries.push(node.final_sql()?);
            }
            Ok(TreeNodeRecursion::Continue)
        });

        let [final_query] = final_queries.as_slice() else {
            panic!("expected a single federated SQL query");
        };

        assert!(final_query.ends_with("/* rewritten by sql_query_rewriter */"));
        assert_eq!(rewrite_calls.load(Ordering::SeqCst), 1);

        Ok(())
    }

    fn test_virtual_execution_plan() -> VirtualExecutionPlan {
        let executor = TestExecutor {
            compute_context: "a".into(),
        };
        let table = get_test_table_provider("table_a1".to_string(), executor.clone());
        let plan = datafusion::logical_expr::LogicalPlanBuilder::scan(
            "table_a1",
            datafusion::datasource::provider_as_source(table),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let statistics = Statistics::new_unknown(plan.schema().as_arrow());
        VirtualExecutionPlan::new(plan, Arc::new(executor), statistics)
    }

    fn a_greater_than(schema: &Schema, value: i64) -> Arc<dyn PhysicalExpr> {
        binary(col("a", schema).unwrap(), Operator::Gt, lit(value), schema).unwrap()
    }

    /// The filters pushed down into a `VirtualExecutionPlan` are not part of its SQL query, and
    /// a `SQLExecutor` may ignore them, so a `FilterExec` on top of it must be kept.
    #[test]
    fn filter_exec_above_virtual_execution_plan_is_kept() {
        let scan: Arc<dyn ExecutionPlan> = Arc::new(test_virtual_execution_plan());
        let predicate = a_greater_than(&scan.schema(), 1);
        let plan = Arc::new(FilterExec::try_new(predicate, scan).unwrap());

        let optimized = FilterPushdown::new()
            .optimize(plan, &ConfigOptions::default())
            .unwrap();
        assert!(
            optimized.downcast_ref::<FilterExec>().is_some(),
            "the FilterExec should be kept:\n{}",
            datafusion::physical_plan::displayable(optimized.as_ref()).indent(true)
        );
    }

    /// Filters pushed down in separate pushdowns, e.g. the dynamic filters of a join and of a
    /// TopK above it, are all passed to the executor.
    #[test]
    fn filters_pushed_down_separately_are_all_kept() {
        let push = |plan: &VirtualExecutionPlan, filter: &Arc<dyn PhysicalExpr>| {
            let result = plan
                .handle_child_pushdown_result(
                    FilterPushdownPhase::Post,
                    ChildPushdownResult {
                        parent_filters: vec![ChildFilterPushdownResult {
                            filter: Arc::clone(filter),
                            child_results: vec![],
                        }],
                        self_filters: vec![],
                    },
                    &ConfigOptions::default(),
                )
                .unwrap();
            result
                .updated_node
                .expect("the filter should be added to the node")
                .downcast_ref::<VirtualExecutionPlan>()
                .unwrap()
                .clone()
        };

        let plan = test_virtual_execution_plan();
        let first = a_greater_than(&plan.schema(), 1);
        let second = a_greater_than(&plan.schema(), 2);
        let plan = push(&push(&plan, &first), &second);

        let filters: Vec<String> = plan.filters.iter().map(|f| f.to_string()).collect();
        assert_eq!(filters, vec![first.to_string(), second.to_string()]);
    }

    /// Records the SQL queries it executes, returning no rows.
    #[derive(Debug, Clone, Default)]
    struct RecordingExecutor {
        queries: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl RecordingExecutor {
        fn queries(&self) -> Vec<String> {
            self.queries.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SQLExecutor for RecordingExecutor {
        fn name(&self) -> &str {
            "RecordingExecutor"
        }

        fn compute_context(&self) -> Option<String> {
            Some("recording".into())
        }

        fn dialect(&self) -> Arc<dyn Dialect> {
            Arc::new(unparser::dialect::DefaultDialect {})
        }

        fn execute(
            &self,
            query: &str,
            schema: SchemaRef,
            _filters: &[Arc<dyn PhysicalExpr>],
        ) -> Result<SendableRecordBatchStream> {
            self.queries.lock().unwrap().push(query.to_string());
            Ok(Box::pin(
                datafusion::physical_plan::EmptyRecordBatchStream::new(schema),
            ))
        }

        async fn table_names(&self) -> Result<Vec<String>> {
            unimplemented!()
        }

        async fn get_table_schema(&self, _table_name: &str) -> Result<SchemaRef> {
            unimplemented!()
        }
    }

    fn recording_scan(executor: &RecordingExecutor, name: &str) -> LogicalPlanBuilder {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let table = Arc::new(RemoteTable::new(
            RemoteTableRef::try_from(name.to_string()).unwrap(),
            schema,
        ));
        let provider = Arc::new(SQLFederationProvider::new(Arc::new(executor.clone())));
        let source = Arc::new(SQLTableSource { provider, table });
        let adaptor = Arc::new(FederatedTableProviderAdaptor::new(source));
        LogicalPlanBuilder::scan(
            name,
            datafusion::datasource::provider_as_source(adaptor),
            None,
        )
        .unwrap()
    }

    fn recording_plan(
        executor: &RecordingExecutor,
        plan: LogicalPlanBuilder,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> VirtualExecutionPlan {
        let plan = plan.build().unwrap();
        let statistics = Statistics::new_unknown(plan.schema().as_arrow());
        let mut plan = VirtualExecutionPlan::new(plan, Arc::new(executor.clone()), statistics);
        plan.filters = filters;
        plan
    }

    #[test]
    fn sql_without_filters_is_not_wrapped() {
        let executor = RecordingExecutor::default();
        let plan = recording_plan(&executor, recording_scan(&executor, "t"), vec![]);
        assert_eq!(
            plan.final_sql_with_filters().unwrap(),
            plan.final_sql().unwrap()
        );
    }

    #[test]
    fn filters_are_applied_to_a_derived_table() {
        let executor = RecordingExecutor::default();
        let plan = recording_scan(&executor, "t");
        let schema = plan.schema().as_arrow().clone();
        let plan = recording_plan(&executor, plan, vec![a_greater_than(&schema, 1)]);
        assert_eq!(
            plan.final_sql_with_filters().unwrap(),
            "SELECT * FROM t AS federated_filtered WHERE (federated_filtered.a > 1)"
        );
    }

    /// Filtering the input of a `LIMIT` would return other rows than filtering its output.
    #[test]
    fn filters_are_applied_after_a_limit() {
        let executor = RecordingExecutor::default();
        let plan = recording_scan(&executor, "t").limit(0, Some(10)).unwrap();
        let schema = plan.schema().as_arrow().clone();
        let plan = recording_plan(&executor, plan, vec![a_greater_than(&schema, 1)]);
        let sql = plan.final_sql_with_filters().unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM (SELECT * FROM t LIMIT 10) AS federated_filtered \
             WHERE (federated_filtered.a > 1)"
        );
    }

    #[test]
    fn untranslatable_filters_leave_the_sql_unchanged() {
        let executor = RecordingExecutor::default();
        let filters: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(datafusion::physical_expr::expressions::NoOp::new()),
            lit(true),
        ];
        let plan = recording_plan(&executor, recording_scan(&executor, "t"), filters);
        assert_eq!(
            plan.final_sql_with_filters().unwrap(),
            plan.final_sql().unwrap()
        );
    }

    /// The derived table deduplicates column names, so filters must be resolved by index.
    #[test]
    fn filters_on_duplicate_column_names_use_the_deduplicated_name() {
        let executor = RecordingExecutor::default();
        let plan = recording_scan(&executor, "t1")
            .cross_join(recording_scan(&executor, "t2").build().unwrap())
            .unwrap();
        let schema = plan.schema().as_arrow().clone();
        // `t2.a`, the second column named `a`.
        let t2_a: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("a", 2));
        let filter = binary(t2_a, Operator::Gt, lit(1i64), &schema).unwrap();
        let plan = recording_plan(&executor, plan, vec![filter]);
        assert_eq!(
            plan.final_sql_with_filters().unwrap(),
            "SELECT * FROM (SELECT t1.a, t1.b, t2.a AS \"a:1\", t2.b AS \"b:1\" \
             FROM t1 CROSS JOIN t2) AS federated_filtered \
             WHERE (federated_filtered.\"a:1\" > 1)"
        );
    }

    #[tokio::test]
    async fn sql_is_generated_when_the_stream_is_first_polled() {
        use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
        use futures::StreamExt;

        let executor = RecordingExecutor::default();
        let plan = recording_scan(&executor, "t");
        let schema = plan.schema().as_arrow().clone();
        let a = col("a", &schema).unwrap();
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(vec![a], lit(true)));
        let plan = recording_plan(&executor, plan, vec![Arc::clone(&dynamic) as _]);

        let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        assert!(
            executor.queries().is_empty(),
            "nothing executed before polling"
        );

        // E.g. a join finishing its build side after the plan was executed.
        dynamic.update(a_greater_than(&schema, 5)).unwrap();
        assert!(stream.next().await.is_none());

        assert_eq!(
            executor.queries(),
            vec![
                "SELECT * FROM t AS federated_filtered WHERE (federated_filtered.a > 5)"
                    .to_string()
            ]
        );
    }

    #[test]
    fn sql_without_filters_is_executed_immediately() {
        let executor = RecordingExecutor::default();
        let plan = recording_plan(&executor, recording_scan(&executor, "t"), vec![]);
        let _stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        assert_eq!(executor.queries(), vec![plan.final_sql().unwrap()]);
    }

    #[test]
    fn display_shows_filters() {
        use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;

        let executor = RecordingExecutor::default();
        let plan = recording_scan(&executor, "t");
        let schema = plan.schema().as_arrow().clone();
        let a = col("a", &schema).unwrap();
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(vec![a], lit(true)));
        let plan = recording_plan(
            &executor,
            plan,
            vec![a_greater_than(&schema, 1), Arc::clone(&dynamic) as _],
        );
        let display = || {
            datafusion::physical_plan::displayable(&plan)
                .one_line()
                .to_string()
        };

        assert!(
            display().ends_with("filters=[a@0 > 1, DynamicFilter [ empty ]]\n"),
            "{}",
            display()
        );
        dynamic.update(a_greater_than(&schema, 5)).unwrap();
        assert!(
            display().ends_with("filters=[a@0 > 1, DynamicFilter [ a@0 > 5 ]]\n"),
            "{}",
            display()
        );
        assert!(!display().contains("federated_filtered"), "{}", display());
    }

    #[test]
    fn display_omits_filters_when_there_are_none() {
        let executor = RecordingExecutor::default();
        let plan = recording_plan(&executor, recording_scan(&executor, "t"), vec![]);
        let display = datafusion::physical_plan::displayable(&plan)
            .one_line()
            .to_string();
        assert!(!display.contains("filters="), "{display}");
    }
}
