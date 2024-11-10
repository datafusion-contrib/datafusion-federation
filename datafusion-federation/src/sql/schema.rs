use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::logical_expr::{TableSource, TableType};
use datafusion::{
    arrow::datatypes::SchemaRef, catalog::SchemaProvider, datasource::TableProvider, error::Result,
};
use futures::future::join_all;

use crate::{
    sql::SQLFederationProvider, FederatedTableProviderAdaptor, FederatedTableSource,
    FederationProvider,
};

#[derive(Debug)]
pub struct SQLSchemaProvider {
    // provider: Arc<SQLFederationProvider>,
    tables: Vec<Arc<SQLTableSource>>,
}

impl SQLSchemaProvider {
    pub async fn new(provider: Arc<SQLFederationProvider>) -> Result<Self> {
        let tables = Arc::clone(&provider).executor.table_names().await?;

        Self::new_with_tables(provider, tables).await
    }

    pub async fn new_with_tables(
        provider: Arc<SQLFederationProvider>,
        tables: Vec<String>,
    ) -> Result<Self> {
        let futures: Vec<_> = tables
            .into_iter()
            .map(|t| SQLTableSource::new(Arc::clone(&provider), t))
            .collect();
        let results: Result<Vec<_>> = join_all(futures).await.into_iter().collect();
        let sources = results?.into_iter().map(Arc::new).collect();
        Ok(Self::new_with_table_sources(sources))
    }

    pub fn new_with_table_sources(tables: Vec<Arc<SQLTableSource>>) -> Self {
        Self { tables }
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

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some(source) = self
            .tables
            .iter()
            .find(|s| s.table_name.eq_ignore_ascii_case(name))
        {
            let adaptor = FederatedTableProviderAdaptor::new(
                Arc::clone(source) as Arc<dyn FederatedTableSource>
            );
            return Ok(Some(Arc::new(adaptor)));
        }
        Ok(None)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .iter()
            .any(|s| s.table_name.eq_ignore_ascii_case(name))
    }
}

#[derive(Debug)]
pub struct MultiSchemaProvider {
    children: Vec<Arc<dyn SchemaProvider>>,
}

impl MultiSchemaProvider {
    pub fn new(children: Vec<Arc<dyn SchemaProvider>>) -> Self {
        Self { children }
    }
}

#[async_trait]
impl SchemaProvider for MultiSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.children.iter().flat_map(|p| p.table_names()).collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        for child in &self.children {
            if let Ok(Some(table)) = child.table(name).await {
                return Ok(Some(table));
            }
        }
        Ok(None)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.children.iter().any(|p| p.table_exist(name))
    }
}

#[derive(Debug)]
pub struct SQLTableSource {
    provider: Arc<SQLFederationProvider>,
    table_name: String,
    schema: SchemaRef,
}

impl SQLTableSource {
    // creates a SQLTableSource and infers the table schema
    pub async fn new(provider: Arc<SQLFederationProvider>, table_name: String) -> Result<Self> {
        let schema = Arc::clone(&provider)
            .executor
            .get_table_schema(table_name.as_str())
            .await?;
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

    pub fn table_name(&self) -> &str {
        self.table_name.as_str()
    }
}

impl FederatedTableSource for SQLTableSource {
    fn federation_provider(&self) -> Arc<dyn FederationProvider> {
        Arc::clone(&self.provider) as Arc<dyn FederationProvider>
    }
}

impl TableSource for SQLTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
}
