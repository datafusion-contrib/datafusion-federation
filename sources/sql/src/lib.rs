use core::fmt;
use std::{any::Any, sync::Arc, vec};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    config::ConfigOptions,
    error::Result,
    execution::{context::SessionState, TaskContext},
    logical_expr::{Extension, LogicalPlan},
    optimizer::analyzer::{Analyzer, AnalyzerRule},
    physical_expr::PhysicalSortExpr,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream},
};
use datafusion_federation::{FederatedPlanNode, FederationPlanner, FederationProvider};
use executor::SQLExecutor;

pub mod executor;
mod schema;
use futures::executor::block_on;
pub use schema::*;

// #[macro_use]
// extern crate derive_builder;

mod producer;
use producer::query_to_sql;

mod ast_builder;

// SQLFederationProvider provides federation to SQL DMBSs.
pub struct SQLFederationProvider {
    analyzer: Arc<Analyzer>,
    executor: Arc<dyn SQLExecutor>,
}

impl SQLFederationProvider {
    pub fn new(executor: Arc<dyn SQLExecutor>) -> Self {
        Self {
            analyzer: Arc::new(Analyzer::with_rules(vec![Arc::new(
                SQLFederationAnalyzerRule::new(executor.clone()),
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
        Some(self.analyzer.clone())
    }
}

struct SQLFederationAnalyzerRule {
    planner: Arc<dyn FederationPlanner>,
}

impl SQLFederationAnalyzerRule {
    pub fn new(executor: Arc<dyn SQLExecutor>) -> Self {
        Self {
            planner: Arc::new(SQLFederationPlanner::new(executor.clone())),
        }
    }
}

impl AnalyzerRule for SQLFederationAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // Simply accept the entire plan for now

        let fed_plan = FederatedPlanNode::new(plan.clone(), self.planner.clone());
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
            self.executor.clone(),
        )))
    }
}

#[derive(Debug, Clone)]
struct VirtualExecutionPlan {
    plan: LogicalPlan,
    executor: Arc<dyn SQLExecutor>,
}

impl VirtualExecutionPlan {
    pub fn new(plan: LogicalPlan, executor: Arc<dyn SQLExecutor>) -> Self {
        Self { plan, executor }
    }

    fn schema(&self) -> SchemaRef {
        let df_schema = self.plan.schema().as_ref();
        Arc::new(Schema::from(df_schema))
    }
}

impl DisplayAs for VirtualExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "VirtualExecutionPlan")
    }
}

impl ExecutionPlan for VirtualExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
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
        let ast = query_to_sql(&self.plan)?;
        let query = format!("{ast}");

        block_on(self.executor.execute(query.as_str()))
    }
}
