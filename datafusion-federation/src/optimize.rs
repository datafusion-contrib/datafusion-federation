use datafusion::{
    common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRewriter},
    error::Result,
    logical_expr::LogicalPlan,
    optimizer::{
        optimizer::ApplyOrder, push_down_filter::PushDownFilter, OptimizerConfig, OptimizerContext, OptimizerRule
    },
};
use optimize_projections::OptimizeProjections;

mod optimize_projections;

#[derive(Debug)]
pub(crate) struct Optimizer {
    config: OptimizerContext,
    push_down_filter: PushDownFilter,
    optimize_projections: OptimizeProjections,
}

impl Default for Optimizer {
    fn default() -> Self {
        // `push_down_filter` and `optimize_projections` does not use config so it can be default
        let config = OptimizerContext::default();

        Self {
            config,
            push_down_filter: PushDownFilter::new(),
            optimize_projections: OptimizeProjections::new(),
        }
    }
}

impl Optimizer {
    pub(crate) fn optimize_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut optimized_plan = plan
            .rewrite(&mut Rewriter::new(
                ApplyOrder::TopDown,
                &self.push_down_filter,
                &self.config,
            ))?
            .data;

        // `optimize_projections` is applied recursively top down so it can be applied only once to the root node
        optimized_plan = self
            .optimize_projections
            .rewrite(optimized_plan, &self.config)
            .data()?;

        Ok(optimized_plan)
    }
}

struct Rewriter<'a> {
    apply_order: ApplyOrder,
    rule: &'a dyn OptimizerRule,
    config: &'a dyn OptimizerConfig,
}

impl<'a> Rewriter<'a> {
    fn new(
        apply_order: ApplyOrder,
        rule: &'a dyn OptimizerRule,
        config: &'a dyn OptimizerConfig,
    ) -> Self {
        Self {
            apply_order,
            rule,
            config,
        }
    }
}

impl<'a> TreeNodeRewriter for Rewriter<'a> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::TopDown {
            optimize_plan_node(node, self.rule, self.config)
        } else {
            Ok(Transformed::no(node))
        }
    }

    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::BottomUp {
            optimize_plan_node(node, self.rule, self.config)
        } else {
            Ok(Transformed::no(node))
        }
    }
}

fn should_run_rule_for_node(node: &LogicalPlan, _rule: &dyn OptimizerRule) -> bool {
    // this logic is applicable only for `push_down_filter_rule`; we don't have any other rules using `should_run_rule_for_node`
    if let LogicalPlan::Filter(x) = node {
        // Applying the `push_down_filter_rule` to certain nodes like `SubqueryAlias`, `Aggregate`, and `CrossJoin`
        // can cause issues during unparsing, thus the optimization is only applied to nodes that are currently supported.
        matches!(
            x.input.as_ref(),
            LogicalPlan::Join(_)
                | LogicalPlan::TableScan(_)
                | LogicalPlan::Projection(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::Distinct(_)
                | LogicalPlan::Sort(_)
        )
    } else {
        true
    }
}

fn optimize_plan_node(
    plan: LogicalPlan,
    rule: &dyn OptimizerRule,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    if !should_run_rule_for_node(&plan, rule) {
        return Ok(Transformed::no(plan));
    }

    if rule.supports_rewrite() {
        return rule.rewrite(plan, config);
    }

    #[allow(deprecated)]
    rule.try_optimize(&plan, config).map(|maybe_plan| {
        match maybe_plan {
            Some(new_plan) => {
                // if the node was rewritten by the optimizer, replace the node
                Transformed::yes(new_plan)
            }
            None => Transformed::no(plan),
        }
    })
}
