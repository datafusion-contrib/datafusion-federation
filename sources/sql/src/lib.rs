use core::fmt;
use std::{any::Any, sync::Arc, vec};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    config::ConfigOptions,
    error::Result,
    execution::{context::SessionState, TaskContext},
    logical_expr::{Extension, LogicalPlan, SubqueryAlias},
    optimizer::analyzer::{Analyzer, AnalyzerRule},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
    },
    sql::{unparser::plan_to_sql, TableReference},
};
use datafusion_federation::{
    get_table_source, FederatedPlanNode, FederationPlanner, FederationProvider,
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
        let plan = rewrite_table_scans(&plan)?;

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
fn rewrite_table_scans(plan: &LogicalPlan) -> Result<LogicalPlan> {
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
                    new_table_scan.table_name = TableReference::from(sql_table_source.table_name());
                }
                None => {
                    // Not a SQLTableSource (is this possible?)
                    return Ok(plan.clone());
                }
            }

            // Wrap the table scan in a SubqueryAlias back to the original table name, so references continue to work.
            let subquery_alias = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(LogicalPlan::TableScan(new_table_scan)),
                original_table_name,
            )?);

            return Ok(subquery_alias);
        } else {
            return Ok(plan.clone());
        }
    }

    let rewritten_inputs = plan
        .inputs()
        .into_iter()
        .map(rewrite_table_scans)
        .collect::<Result<Vec<_>>>()?;

    let new_plan = plan.with_new_exprs(plan.expressions(), rewritten_inputs)?;

    Ok(new_plan)
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
        Ok(Arc::new(VirtualExecutionPlan::new(
            node.plan().clone(),
            Arc::clone(&self.executor),
        )))
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
        let ast = plan_to_sql(&self.plan)?;
        let query = format!("{ast}");

        self.executor.execute(query.as_str(), self.schema())
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }
}
