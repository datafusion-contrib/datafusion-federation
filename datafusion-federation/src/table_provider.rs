use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::Constraints,
    datasource::TableProvider,
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{Expr, LogicalPlan, TableSource, TableType},
    physical_plan::ExecutionPlan,
};

use crate::FederationProvider;

// FederatedTableSourceWrapper helps to recover the FederatedTableSource
// from a TableScan. This wrapper may be avoidable.
pub struct FederatedTableProviderAdaptor {
    pub source: Arc<dyn FederatedTableSource>,
}

impl FederatedTableProviderAdaptor {
    pub fn new(source: Arc<dyn FederatedTableSource>) -> Self {
        Self { source }
    }
}

#[async_trait]
impl TableProvider for FederatedTableProviderAdaptor {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.source.schema()
    }
    fn constraints(&self) -> Option<&Constraints> {
        self.source.constraints()
    }
    fn table_type(&self) -> TableType {
        self.source.table_type()
    }
    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        self.source.get_logical_plan()
    }
    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.source.get_column_default(column)
    }

    // Scan is not supported; the adaptor should be replaced
    // with a virtual TableProvider that provides federation for a sub-plan.
    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "FederatedTableProviderAdaptor cannot scan".to_string(),
        ))
    }
}

// FederatedTableProvider extends DataFusion's TableProvider trait
// to allow grouping of TableScans of the same FederationProvider.
#[async_trait]
pub trait FederatedTableSource: TableSource {
    // Return the FederationProvider associated with this Table
    fn federation_provider(&self) -> Arc<dyn FederationProvider>;
}
