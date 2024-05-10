use std::sync::Arc;

use datafusion::common::not_impl_err;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::optimizer::analyzer::Analyzer;
use datafusion::{
    config::ConfigOptions,
    datasource::source_as_provider,
    error::Result,
    logical_expr::{Expr, LogicalPlan, Projection, TableScan, TableSource},
    optimizer::analyzer::AnalyzerRule,
};

use crate::{
    FederatedTableProviderAdaptor, FederatedTableSource, FederationProvider, FederationProviderRef,
};

#[derive(Default)]
pub struct FederationAnalyzerRule {}

impl AnalyzerRule for FederationAnalyzerRule {
    // Walk over the plan, look for the largest subtrees that only have
    // TableScans from the same FederationProvider.
    // There 'largest sub-trees' are passed to their respective FederationProvider.optimizer.
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let (optimized, _) = self.optimize_plan_recursively(&plan, true, config)?;
        if let Some(result) = optimized {
            return Ok(result);
        }
        Ok(plan.clone())
    }

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str {
        "federation_optimizer_rule"
    }
}

// tri-state:
//    None: no providers
//    Some(None): ambiguous
//    Some(Some(provider)): sole provider
type ScanResult = Option<Option<FederationProviderRef>>;

impl FederationAnalyzerRule {
    pub fn new() -> Self {
        Self::default()
    }

    // scans a plan to see if it belongs to a single FederationProvider
    fn scan_plan_recursively(&self, plan: &LogicalPlan) -> Result<ScanResult> {
        let mut sole_provider: ScanResult = None;

        plan.apply(&mut |p: &LogicalPlan| -> Result<TreeNodeRecursion> {
            let exprs_provider = self.scan_plan_exprs(plan)?;
            let recursion = merge_scan_result(&mut sole_provider, exprs_provider);
            if recursion == TreeNodeRecursion::Stop {
                return Ok(recursion);
            }

            let sub_provider = get_leaf_provider(p)?;
            Ok(proc_scan_result(&mut sole_provider, sub_provider))
        })?;

        Ok(sole_provider)
    }

    // scans a plan's expressions to see if it belongs to a single FederationProvider
    fn scan_plan_exprs(&self, plan: &LogicalPlan) -> Result<ScanResult> {
        let mut sole_provider: ScanResult = None;

        let exprs = plan.expressions();
        for expr in &exprs {
            let expr_result = self.scan_expr_recursively(expr)?;
            let recursion = merge_scan_result(&mut sole_provider, expr_result);
            if recursion == TreeNodeRecursion::Stop {
                return Ok(sole_provider);
            }
        }

        Ok(sole_provider)
    }

    // scans an expression to see if it belongs to a single FederationProvider
    fn scan_expr_recursively(&self, expr: &Expr) -> Result<ScanResult> {
        let mut sole_provider: ScanResult = None;

        expr.apply(&mut |e: &Expr| -> Result<TreeNodeRecursion> {
            // TODO: Support other types of sub-queries
            if let Expr::ScalarSubquery(ref subquery) = e {
                let plan_result = self.scan_plan_recursively(&subquery.subquery)?;
                Ok(merge_scan_result(&mut sole_provider, plan_result))
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;

        Ok(sole_provider)
    }

    // optimize_recursively recursively finds the largest sub-plans that can be federated
    // to a single FederationProvider.
    // Returns a plan if a sub-tree was federated, otherwise None.
    // Returns a FederationProvider if it covers the entire sub-tree, otherwise None.
    fn optimize_plan_recursively(
        &self,
        plan: &LogicalPlan,
        is_root: bool,
        _config: &ConfigOptions,
    ) -> Result<(Option<LogicalPlan>, ScanResult)> {
        // Used to track if all sources, including tableScan, plan inputs and
        // expressions, represents an un-ambiguous or 'sole' FederationProvider
        let mut sole_provider: ScanResult = None;

        // Check if this plan node is a leaf that determines the FederationProvider
        let leaf_provider = get_leaf_provider(plan)?;

        // Check if the expressions contain, a potentially different, FederationProvider
        let exprs_result = self.scan_plan_exprs(plan)?;
        let optimize_expressions = exprs_result.is_some();

        // Return early if this is a leaf and there is no ambiguity with the expressions.
        if leaf_provider.is_some()
            && (exprs_result.is_none() || scan_result_eq(&exprs_result, &leaf_provider))
        {
            return Ok((None, Some(leaf_provider)));
        }
        // Aggregate leaf & expression providers
        proc_scan_result(&mut sole_provider, leaf_provider);
        merge_scan_result(&mut sole_provider, exprs_result);

        let inputs = plan.inputs();
        // Return early if there are no sources.
        if inputs.is_empty() && sole_provider.is_none() {
            return Ok((None, None));
        }

        // Recursively optimize inputs
        let input_results = inputs
            .iter()
            .map(|i| self.optimize_plan_recursively(i, false, _config))
            .collect::<Result<Vec<_>>>()?;

        // Aggregate the input providers
        input_results.iter().for_each(|(_, scan_result)| {
            merge_scan_result(&mut sole_provider, scan_result.clone());
        });

        let Some(sole_provider) = sole_provider else {
            // No providers found
            // TODO: Is/should this be reachable?
            return Ok((None, None));
        };

        // If all sources are federated to the same provider
        if let Some(provider) = sole_provider {
            if !is_root {
                // The largest sub-plan is higher up.
                return Ok((None, Some(Some(provider))));
            }

            let Some(optimizer) = provider.analyzer() else {
                // No optimizer provided
                return Ok((None, None));
            };

            // If this is the root plan node; federate the entire plan
            let optimized = optimizer.execute_and_check(plan, _config, |_, _| {})?;
            return Ok((Some(optimized), None));
        }

        // The plan is ambiguous; any input that is not yet optimized and has a
        // sole provider represents a largest sub-plan and should be federated.
        //
        // We loop over the input optimization results, federate where needed and
        // return a complete list of new inputs for the optimized plan.
        let new_inputs = input_results
            .into_iter()
            .enumerate()
            .map(|(i, (input_plan, provider))| {
                if let Some(federated_plan) = input_plan {
                    // Already federated deeper in the plan tree
                    return Ok(federated_plan);
                }
                let provider = provider.unwrap();
                let original_input = (*inputs.get(i).unwrap()).clone();
                let Some(provider) = provider else {
                    // No provider for this input; use the original input.
                    return Ok(original_input);
                };

                let Some(optimizer) = provider.analyzer() else {
                    // No optimizer for this input; use the original input.
                    return Ok(original_input);
                };

                // Replace the input with the federated counterpart
                let wrapped = wrap_projection(original_input)?;
                let optimized = optimizer.execute_and_check(&wrapped, _config, |_, _| {})?;

                Ok(optimized)
            })
            .collect::<Result<Vec<_>>>()?;

        // Optimize expressions if needed
        let new_expressions = if optimize_expressions {
            self.optimize_plan_exprs(plan, _config)?
        } else {
            plan.expressions()
        };

        // Construct the optimized plan
        let new_plan = plan.with_new_exprs(new_expressions, new_inputs)?;

        // Return the federated plan and Some(None) meaning "ambiguous provider"
        Ok((Some(new_plan), Some(None)))
    }

    // Optimize all exprs of a plan
    fn optimize_plan_exprs(
        &self,
        plan: &LogicalPlan,
        _config: &ConfigOptions,
    ) -> Result<Vec<Expr>> {
        plan.expressions()
            .iter()
            .map(|expr| {
                let transformed = expr
                    .clone()
                    .transform(&|e| self.optimize_expr_recursively(e, _config))?;
                Ok(transformed.data)
            })
            .collect::<Result<Vec<_>>>()
    }

    // recursively optimize expressions
    // Current logic: individually federate every sub-query.
    fn optimize_expr_recursively(
        &self,
        expr: Expr,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        match expr {
            Expr::ScalarSubquery(ref subquery) => {
                // Optimize as root to force federating the sub-query
                let (new_subquery, _) =
                    self.optimize_plan_recursively(&subquery.subquery, true, _config)?;
                let Some(new_subquery) = new_subquery else {
                    return Ok(Transformed::no(expr));
                };
                Ok(Transformed::yes(Expr::ScalarSubquery(
                    subquery.with_plan(new_subquery.into()),
                )))
            }
            Expr::InSubquery(_) => not_impl_err!("InSubquery"),
            _ => Ok(Transformed::no(expr)),
        }
    }
}

fn scan_result_eq(result: &ScanResult, provider: &Option<FederationProviderRef>) -> bool {
    match (result, provider) {
        (None, _) => false,
        (Some(left), right) => left == right,
    }
}

fn merge_scan_result(result: &mut ScanResult, other: ScanResult) -> TreeNodeRecursion {
    match (&result, other) {
        (_, None) => TreeNodeRecursion::Continue,
        (_, Some(None)) => {
            *result = Some(None);
            TreeNodeRecursion::Stop
        }
        (_, Some(other)) => proc_scan_result(result, other),
    }
}

fn proc_scan_result(
    result: &mut ScanResult,
    provider: Option<FederationProviderRef>,
) -> TreeNodeRecursion {
    match (&result, provider) {
        (_, None) => TreeNodeRecursion::Continue, // No provider in this plan
        (Some(None), _) => {
            // Should be unreadable
            TreeNodeRecursion::Stop
        }
        (None, Some(provider)) => {
            *result = Some(Some(provider));
            TreeNodeRecursion::Continue
        }
        (Some(Some(result_unwrapped)), Some(provider_unwrapped)) => {
            if *result_unwrapped == provider_unwrapped {
                TreeNodeRecursion::Continue
            } else {
                *result = Some(None);
                TreeNodeRecursion::Stop
            }
        }
    }
}

// NopFederationProvider is used to represent tables that are not federated, but
// are resolved by DataFusion. This simplifies the logic of the optimizer rule.
struct NopFederationProvider {}

impl FederationProvider for NopFederationProvider {
    fn name(&self) -> &str {
        "nop"
    }

    fn compute_context(&self) -> Option<String> {
        None
    }

    fn analyzer(&self) -> Option<Arc<Analyzer>> {
        None
    }
}

fn get_leaf_provider(plan: &LogicalPlan) -> Result<Option<FederationProviderRef>> {
    match plan {
        LogicalPlan::TableScan(TableScan { ref source, .. }) => {
            let Some(federated_source) = get_table_source(source)? else {
                // Table is not federated but provided by a standard table provider.
                // We use a placeholder federation provider to simplify the logic.
                return Ok(Some(Arc::new(NopFederationProvider {})));
            };
            let provider = federated_source.federation_provider();
            Ok(Some(provider))
        }
        _ => Ok(None),
    }
}

fn wrap_projection(plan: LogicalPlan) -> Result<LogicalPlan> {
    // TODO: minimize requested columns
    match plan {
        LogicalPlan::Projection(_) => Ok(plan),
        _ => {
            let expr = plan
                .schema()
                .fields()
                .iter()
                .map(|f| Expr::Column(f.qualified_column()))
                .collect::<Vec<Expr>>();
            Ok(LogicalPlan::Projection(Projection::try_new(
                expr,
                Arc::new(plan),
            )?))
        }
    }
}

pub fn get_table_source(
    source: &Arc<dyn TableSource>,
) -> Result<Option<Arc<dyn FederatedTableSource>>> {
    // Unwrap TableSource
    let source = source_as_provider(source)?;

    // Get FederatedTableProviderAdaptor
    let Some(wrapper) = source
        .as_any()
        .downcast_ref::<FederatedTableProviderAdaptor>()
    else {
        return Ok(None);
    };

    // Return original FederatedTableSource
    Ok(Some(Arc::clone(&wrapper.source)))
}
