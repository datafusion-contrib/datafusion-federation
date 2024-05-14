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
    pub table_provider: Option<Arc<dyn TableProvider>>,
}

impl FederatedTableProviderAdaptor {
    pub fn new(source: Arc<dyn FederatedTableSource>) -> Self {
        Self {
            source,
            table_provider: None,
        }
    }

    /// Creates a new FederatedTableProviderAdaptor that falls back to the
    /// provided TableProvider. This is useful if used within a DataFusion
    /// context without the federation optimizer.
    pub fn new_with_provider(
        source: Arc<dyn FederatedTableSource>,
        table_provider: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            source,
            table_provider: Some(table_provider),
        }
    }
}

#[async_trait]
impl TableProvider for FederatedTableProviderAdaptor {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        if let Some(table_provider) = &self.table_provider {
            return table_provider.schema();
        }

        self.source.schema()
    }
    fn constraints(&self) -> Option<&Constraints> {
        if let Some(table_provider) = &self.table_provider {
            return table_provider
                .constraints()
                .or_else(|| self.source.constraints());
        }

        self.source.constraints()
    }
    fn table_type(&self) -> TableType {
        if let Some(table_provider) = &self.table_provider {
            return table_provider.table_type();
        }

        self.source.table_type()
    }
    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        if let Some(table_provider) = &self.table_provider {
            return table_provider
                .get_logical_plan()
                .or_else(|| self.source.get_logical_plan());
        }

        self.source.get_logical_plan()
    }
    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        if let Some(table_provider) = &self.table_provider {
            return table_provider
                .get_column_default(column)
                .or_else(|| self.source.get_column_default(column));
        }

        self.source.get_column_default(column)
    }

    // Scan is not supported; the adaptor should be replaced
    // with a virtual TableProvider that provides federation for a sub-plan.
    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(table_provider) = &self.table_provider {
            return table_provider.scan(state, projection, filters, limit).await;
        }

        Err(DataFusionError::NotImplemented(
            "FederatedTableProviderAdaptor cannot scan".to_string(),
        ))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(table_provider) = &self.table_provider {
            return table_provider.insert_into(_state, input, overwrite).await;
        }

        Err(DataFusionError::NotImplemented(
            "FederatedTableProviderAdaptor cannot insert_into".to_string(),
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
