use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRewriter},
    error::Result,
    logical_expr:: LogicalPlan,
    optimizer::{
        optimizer::ApplyOrder, push_down_filter, OptimizerConfig, OptimizerContext, OptimizerRule
    }
};

pub(crate) fn optimize_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
    let push_down_filter_rule = push_down_filter::PushDownFilter::new();
    // `push_down_filter` does not use config so it can be default
    let optimizer_config = OptimizerContext::default();

    let res = match push_down_filter_rule.apply_order() {
        Some(apply_order) => plan.rewrite(&mut Rewriter::new(
            apply_order,
            &push_down_filter_rule,
            &optimizer_config,
        )),
        None => optimize_plan_node(plan, &push_down_filter_rule, &optimizer_config),
    };

    Ok(res?.data)
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
    // this logic is applicable only for `push_down_filter_rule`; we don't have any other rules
    if let LogicalPlan::Filter(x) = node {
        // Applying the `push_down_filter_rule` to certain nodes like `SubqueryAlias`, `Aggregate`, and `CrossJoin`
        // can cause issues during unparsing, thus the optimization is only applied to nodes that are currently supported.
        matches!(
            x.input.as_ref(),
            LogicalPlan::Join(_) | LogicalPlan::TableScan(_) | LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::Distinct(_) | LogicalPlan::Sort(_)
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
