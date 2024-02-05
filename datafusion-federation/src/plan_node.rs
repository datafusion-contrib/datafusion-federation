use core::fmt;
use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{
    common::DFSchemaRef,
    error::Result,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};

pub struct FederatedPlanNode {
    plan: LogicalPlan,
    planner: Arc<dyn FederationPlanner>,
}

impl FederatedPlanNode {
    pub fn new(plan: LogicalPlan, planner: Arc<dyn FederationPlanner>) -> Self {
        Self { plan, planner }
    }

    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }
}

impl Debug for FederatedPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for FederatedPlanNode {
    fn name(&self) -> &str {
        "Federated"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        self.plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Federated")
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 0, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Self {
            plan: self.plan.clone(),
            planner: self.planner.clone(),
        }
    }
}

#[derive(Default)]

pub struct FederatedQueryPlanner {}

impl FederatedQueryPlanner {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl QueryPlanner for FederatedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Get provider here?

        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![
                Arc::new(FederatedPlanner::new()),
            ]);
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

#[async_trait]
pub trait FederationPlanner: Send + Sync {
    async fn plan_federation(
        &self,
        node: &FederatedPlanNode,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

impl PartialEq<FederatedPlanNode> for FederatedPlanNode {
    /// Comparing name, args and return_type
    fn eq(&self, other: &FederatedPlanNode) -> bool {
        self.plan == other.plan
    }
}

impl Eq for FederatedPlanNode {}

impl Hash for FederatedPlanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.plan.hash(state);
    }
}

#[derive(Default)]
struct FederatedPlanner {}

impl FederatedPlanner {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ExtensionPlanner for FederatedPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let dc_node = node.as_any().downcast_ref::<FederatedPlanNode>();
        if let Some(fed_node) = dc_node {
            assert_eq!(logical_inputs.len(), 0, "Inconsistent number of inputs");
            assert_eq!(physical_inputs.len(), 0, "Inconsistent number of inputs");

            let fed_planner = fed_node.planner.clone();
            let exec_plan = fed_planner.plan_federation(fed_node, session_state).await?;
            return Ok(Some(exec_plan));
        }
        Ok(None)
    }
}
