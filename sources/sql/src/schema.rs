use async_trait::async_trait;
use datafusion::logical_expr::{TableSource, TableType};
use datafusion::{
    arrow::datatypes::SchemaRef, catalog::schema::SchemaProvider, datasource::TableProvider,
    error::Result,
};
use futures::future::join_all;
use std::{any::Any, sync::Arc};

use datafusion_federation::{
    FederatedTableProviderAdaptor, FederatedTableSource, FederationProvider,
};

use crate::SQLFederationProvider;

pub struct SQLSchemaProvider {
    // provider: Arc<SQLFederationProvider>,
    tables: Vec<Arc<SQLTableSource>>,
}

impl SQLSchemaProvider {
    pub async fn new(provider: Arc<SQLFederationProvider>, tables: Vec<String>) -> Result<Self> {
        let futures: Vec<_> = tables
            .into_iter()
            .map(|t| SQLTableSource::new(provider.clone(), t))
            .collect();
        let results: Result<Vec<_>> = join_all(futures).await.into_iter().collect();
        let sources = results?.into_iter().map(Arc::new).collect();
        Ok(Self {
            // provider,
            tables: sources,
        })
    }
}

#[async_trait]
impl SchemaProvider for SQLSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.iter().map(|s| s.table_name.clone()).collect()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        if let Some(source) = self
            .tables
            .iter()
            .find(|s| s.table_name.eq_ignore_ascii_case(name))
        {
            let adaptor = FederatedTableProviderAdaptor::new(source.clone());
            return Some(Arc::new(adaptor));
        }
        None
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .iter()
            .any(|s| s.table_name.eq_ignore_ascii_case(name))
    }
}

struct SQLTableSource {
    provider: Arc<SQLFederationProvider>,
    table_name: String,
    schema: SchemaRef,
}

impl SQLTableSource {
    // creates a SQLTableSource and infers the table schema
    pub async fn new(provider: Arc<SQLFederationProvider>, table_name: String) -> Result<Self> {
        // Simple schema inference
        let query = format!("SELECT * FROM {table_name} LIMIT 1");
        let schema = provider
            .clone()
            .executor
            .execute(query.as_str())
            .await?
            .schema();

        Self::new_with_schema(provider, table_name, schema)
    }

    pub fn new_with_schema(
        provider: Arc<SQLFederationProvider>,
        table_name: String,
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            provider,
            table_name,
            schema,
        })
    }
}

impl FederatedTableSource for SQLTableSource {
    fn federation_provider(&self) -> Arc<dyn FederationProvider> {
        self.provider.clone()
    }
}

impl TableSource for SQLTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
}
