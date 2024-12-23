mod scan_result;

use crate::FederationProvider;
use crate::{
    optimize::Optimizer, FederatedTableProviderAdaptor, FederatedTableSource, FederationProviderRef,
};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{col, expr::InSubquery, LogicalPlanBuilder};
use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    config::ConfigOptions,
    datasource::source_as_provider,
    error::Result,
    logical_expr::{Expr, Extension, LogicalPlan, Projection, TableScan, TableSource},
    optimizer::analyzer::AnalyzerRule,
    sql::TableReference,
};
use scan_result::ScanResult;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Debug)]
pub struct FederationAnalyzerRule {
    optimizer: Optimizer,
    provider_map: Arc<RwLock<HashMap<TableReference, ScanResult>>>,
}

impl Default for FederationAnalyzerRule {
    fn default() -> Self {
        Self {
            optimizer: Optimizer::default(),
            provider_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl AnalyzerRule for FederationAnalyzerRule {
    // Walk over the plan, look for the largest subtrees that only have
    // TableScans from the same FederationProvider.
    // There 'largest sub-trees' are passed to their respective FederationProvider.optimizer.
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if !contains_federated_table(&plan)? {
            return Ok(plan);
        }
        // Run selected optimizer rules before federation
        let plan = self.optimizer.optimize_plan(plan)?;

        // Find all federation providers for TableReference appeared in the plan
        let providers = get_plan_provider_recursively(&plan)?;
        let mut write_map = self.provider_map.write().map_err(|_| {
            DataFusionError::External(
                "Failed to create federated plan: failed to find all federated providers.".into(),
            )
        })?;
        write_map.extend(providers);
        drop(write_map);

        match self.optimize_plan_recursively(&plan, true, config)? {
            (Some(optimized_plan), _) => Ok(optimized_plan),
            (None, _) => Ok(plan),
        }
    }

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str {
        "federation_optimizer_rule"
    }
}

impl FederationAnalyzerRule {
    pub fn new() -> Self {
        Self::default()
    }

    /// Scans a plan to see if it belongs to a single [`FederationProvider`].
    fn scan_plan_recursively(&self, plan: &LogicalPlan) -> Result<ScanResult> {
        let mut sole_provider: ScanResult = ScanResult::None;

        plan.apply(&mut |p: &LogicalPlan| -> Result<TreeNodeRecursion> {
            let exprs_provider = self.scan_plan_exprs(p)?;
            sole_provider.merge(exprs_provider);

            if sole_provider.is_ambiguous() {
                return Ok(TreeNodeRecursion::Stop);
            }

            let (sub_provider, _) = get_leaf_provider(p)?;
            sole_provider.add(sub_provider);

            Ok(sole_provider.check_recursion())
        })?;

        Ok(sole_provider)
    }

    /// Scans a plan's expressions to see if it belongs to a single [`FederationProvider`].
    fn scan_plan_exprs(&self, plan: &LogicalPlan) -> Result<ScanResult> {
        let mut sole_provider: ScanResult = ScanResult::None;

        let exprs = plan.expressions();
        for expr in &exprs {
            let expr_result = self.scan_expr_recursively(expr)?;
            sole_provider.merge(expr_result);

            if sole_provider.is_ambiguous() {
                return Ok(sole_provider);
            }
        }

        Ok(sole_provider)
    }

    /// scans an expression to see if it belongs to a single [`FederationProvider`]
    fn scan_expr_recursively(&self, expr: &Expr) -> Result<ScanResult> {
        let mut sole_provider: ScanResult = ScanResult::None;

        expr.apply(&mut |e: &Expr| -> Result<TreeNodeRecursion> {
            // TODO: Support other types of sub-queries
            match e {
                Expr::ScalarSubquery(ref subquery) => {
                    let plan_result = self.scan_plan_recursively(&subquery.subquery)?;

                    sole_provider.merge(plan_result);
                    Ok(sole_provider.check_recursion())
                }
                Expr::InSubquery(ref insubquery) => {
                    let plan_result = self.scan_plan_recursively(&insubquery.subquery.subquery)?;

                    sole_provider.merge(plan_result);
                    Ok(sole_provider.check_recursion())
                }
                Expr::OuterReferenceColumn(_, ref col) => {
                    if let Some(table) = &col.relation {
                        let map = self.provider_map.read().map_err(|_| {
                            DataFusionError::External(
                                "Failed to create federated plan: failed to obtain a read lock on federated providers.".into(),
                            )
                        })?;
                        if let Some(plan_result) = map.get(table) {
                            sole_provider.merge(plan_result.clone());
                            return Ok(sole_provider.check_recursion());
                        }
                    }
                    // Subqueries that reference outer columns are not supported
                    // for now. We handle this here as ambiguity to force
                    // federation lower in the plan tree.
                    sole_provider = ScanResult::Ambiguous;
                    Ok(TreeNodeRecursion::Stop)
                }
                _ => Ok(TreeNodeRecursion::Continue),
            }
        })?;

        Ok(sole_provider)
    }

    /// Recursively finds the largest sub-plans that can be federated
    /// to a single FederationProvider.
    ///
    /// Returns a plan if a sub-tree was federated, otherwise None.
    ///
    /// Returns a ScanResult of all FederationProviders in the subtree.
    fn optimize_plan_recursively(
        &self,
        plan: &LogicalPlan,
        is_root: bool,
        _config: &ConfigOptions,
    ) -> Result<(Option<LogicalPlan>, ScanResult)> {
        let mut sole_provider: ScanResult = ScanResult::None;

        if let LogicalPlan::Extension(Extension { ref node }) = plan {
            if node.name() == "Federated" {
                // Avoid attempting double federation
                return Ok((None, ScanResult::Ambiguous));
            }
        }

        // Check if this plan node is a leaf that determines the FederationProvider
        let (leaf_provider, _) = get_leaf_provider(plan)?;

        // Check if the expressions contain, a potentially different, FederationProvider
        let exprs_result = self.scan_plan_exprs(plan)?;

        // Return early if this is a leaf and there is no ambiguity with the expressions.
        if leaf_provider.is_some() && (exprs_result.is_none() || exprs_result == leaf_provider) {
            return Ok((None, leaf_provider.into()));
        }
        // Aggregate leaf & expression providers
        sole_provider.add(leaf_provider);
        sole_provider.merge(exprs_result.clone());

        let inputs = plan.inputs();
        // Return early if there are no sources.
        if inputs.is_empty() && sole_provider.is_none() {
            return Ok((None, ScanResult::None));
        }

        // Recursively optimize inputs
        let input_results = inputs
            .iter()
            .map(|i| self.optimize_plan_recursively(i, false, _config))
            .collect::<Result<Vec<_>>>()?;

        // Aggregate the input providers
        input_results.iter().for_each(|(_, scan_result)| {
            sole_provider.merge(scan_result.clone());
        });

        if sole_provider.is_none() {
            // No providers found
            // TODO: Is/should this be reachable?
            return Ok((None, ScanResult::None));
        }

        // Federate Exprs when Exprs provider is ambiguous or Exprs provider differs from the sole_provider of current plan
        // When Exprs provider is the same as sole_provider and non-ambiguous, the larger sub-plan is higher up
        let optimize_expressions = exprs_result.is_some()
            && (!(sole_provider == exprs_result) || exprs_result.is_ambiguous());

        // If all sources are federated to the same provider
        if let ScanResult::Distinct(provider) = sole_provider {
            if !is_root {
                // The largest sub-plan is higher up.
                return Ok((None, ScanResult::Distinct(provider)));
            }

            let Some(optimizer) = provider.analyzer() else {
                // No optimizer provided
                return Ok((None, ScanResult::None));
            };

            // If this is the root plan node; federate the entire plan
            let optimized = optimizer.execute_and_check(plan.clone(), _config, |_, _| {})?;
            return Ok((Some(optimized), ScanResult::None));
        }

        // The plan is ambiguous; any input that is not yet optimized and has a
        // sole provider represents a largest sub-plan and should be federated.
        //
        // We loop over the input optimization results, federate where needed and
        // return a complete list of new inputs for the optimized plan.
        let new_inputs = input_results
            .into_iter()
            .enumerate()
            .map(|(i, (input_plan, input_result))| {
                if let Some(federated_plan) = input_plan {
                    // Already federated deeper in the plan tree
                    return Ok(federated_plan);
                }

                let original_input = (*inputs.get(i).unwrap()).clone();
                if input_result.is_ambiguous() {
                    // Can happen if the input is already federated, so use
                    // the original input.
                    return Ok(original_input);
                }

                let provider = input_result.unwrap()?;
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
                let optimized = optimizer.execute_and_check(wrapped, _config, |_, _| {})?;

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

        // Return the federated plan
        Ok((Some(new_plan), ScanResult::Ambiguous))
    }

    /// Optimizes all exprs of a plan
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

    /// recursively optimize expressions
    /// Current logic: individually federate every sub-query.
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

                // ScalarSubqueryToJoin optimizer rule doesn't support federated node (LogicalPlan::Extension(_)) as subquery
                // Wrap a `non-op` Projection LogicalPlan outside the federated node to facilitate ScalarSubqueryToJoin optimization
                if matches!(new_subquery, LogicalPlan::Extension(_)) {
                    let all_columns = new_subquery
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| col(field.name()))
                        .collect::<Vec<_>>();

                    let projection_plan = LogicalPlanBuilder::from(new_subquery)
                        .project(all_columns)?
                        .build()?;

                    return Ok(Transformed::yes(Expr::ScalarSubquery(
                        subquery.with_plan(projection_plan.into()),
                    )));
                }

                Ok(Transformed::yes(Expr::ScalarSubquery(
                    subquery.with_plan(new_subquery.into()),
                )))
            }
            Expr::InSubquery(ref in_subquery) => {
                let (new_subquery, _) =
                    self.optimize_plan_recursively(&in_subquery.subquery.subquery, true, _config)?;
                let Some(new_subquery) = new_subquery else {
                    return Ok(Transformed::no(expr));
                };

                // DecorrelatePredicateSubquery  optimizer rule doesn't support federated node (LogicalPlan::Extension(_)) as subquery
                // Wrap a `non-op` Projection LogicalPlan outside the federated node to facilitate DecorrelatePredicateSubquery optimization
                if matches!(new_subquery, LogicalPlan::Extension(_)) {
                    let all_columns = new_subquery
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| col(field.name()))
                        .collect::<Vec<_>>();

                    let projection_plan = LogicalPlanBuilder::from(new_subquery)
                        .project(all_columns)?
                        .build()?;

                    return Ok(Transformed::yes(Expr::InSubquery(InSubquery::new(
                        in_subquery.expr.clone(),
                        in_subquery.subquery.with_plan(projection_plan.into()),
                        in_subquery.negated,
                    ))));
                }

                Ok(Transformed::yes(Expr::InSubquery(InSubquery::new(
                    in_subquery.expr.clone(),
                    in_subquery.subquery.with_plan(new_subquery.into()),
                    in_subquery.negated,
                ))))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }
}

/// NopFederationProvider is used to represent tables that are not federated, but
/// are resolved by DataFusion. This simplifies the logic of the optimizer rule.
struct NopFederationProvider {}

impl FederationProvider for NopFederationProvider {
    fn name(&self) -> &str {
        "nop"
    }

    fn compute_context(&self) -> Option<String> {
        None
    }

    fn analyzer(&self) -> Option<Arc<datafusion::optimizer::Analyzer>> {
        None
    }
}

/// Recursively find the [`FederationProvider`] for all [`TableReference`] instances in the plan.
/// This information is used to resolve the federation provider for [`Expr::OuterReferenceColumn`].
fn get_plan_provider_recursively(
    plan: &LogicalPlan,
) -> Result<HashMap<TableReference, ScanResult>> {
    let mut providers: HashMap<TableReference, ScanResult> = HashMap::new();

    plan.apply(&mut |p: &LogicalPlan| -> Result<TreeNodeRecursion> {
        // LogicalPlan::SubqueryAlias can also be referred by OuterReferenceColumn
        // Get the federation provider for TableReference representing LogicalPlan::SubqueryAlias
        if let LogicalPlan::SubqueryAlias(a) = p {
            let subquery_alias_providers = get_plan_provider_recursively(&Arc::clone(&a.input))?;
            let mut provider: ScanResult = ScanResult::None;
            for (_, i) in subquery_alias_providers {
                provider.merge(i);
            }
            providers.insert(a.alias.clone(), provider);
        }

        let (federation_provider, table_reference) = get_leaf_provider(p)?;
        if let Some(table_reference) = table_reference {
            providers.insert(table_reference, federation_provider.into());
        }

        let _ = p.apply_subqueries(|sub_query| {
            let subquery_providers = get_plan_provider_recursively(sub_query)?;
            providers.extend(subquery_providers);
            Ok(TreeNodeRecursion::Continue)
        });

        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(providers)
}

fn wrap_projection(plan: LogicalPlan) -> Result<LogicalPlan> {
    // TODO: minimize requested columns
    match plan {
        LogicalPlan::Projection(_) => Ok(plan),
        _ => {
            let expr = plan
                .schema()
                .columns()
                .iter()
                .map(|c| Expr::Column(c.clone()))
                .collect::<Vec<Expr>>();
            Ok(LogicalPlan::Projection(Projection::try_new(
                expr,
                Arc::new(plan),
            )?))
        }
    }
}

fn contains_federated_table(plan: &LogicalPlan) -> Result<bool> {
    let federated_table_exists = plan.exists(|x| {
        if let (Some(provider), _) = get_leaf_provider(x)? {
            // federated table provider should have an analyzer
            return Ok(provider.analyzer().is_some());
        }
        Ok(false)
    })?;

    Ok(federated_table_exists)
}

fn get_leaf_provider(
    plan: &LogicalPlan,
) -> Result<(Option<FederationProviderRef>, Option<TableReference>)> {
    match plan {
        LogicalPlan::TableScan(TableScan {
            ref table_name,
            ref source,
            ..
        }) => {
            let table_reference = table_name.clone();
            let Some(federated_source) = get_table_source(source)? else {
                // Table is not federated but provided by a standard table provider.
                // We use a placeholder federation provider to simplify the logic.
                return Ok((
                    Some(Arc::new(NopFederationProvider {})),
                    Some(table_reference),
                ));
            };
            let provider = federated_source.federation_provider();
            Ok((Some(provider), Some(table_reference)))
        }
        _ => Ok((None, None)),
    }
}

#[allow(clippy::missing_errors_doc)]
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
