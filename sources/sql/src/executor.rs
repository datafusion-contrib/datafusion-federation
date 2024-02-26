use async_trait::async_trait;
use core::fmt;
use datafusion::{
    arrow::datatypes::SchemaRef, error::Result, physical_plan::SendableRecordBatchStream,
};
use std::sync::Arc;

pub type SQLExecutorRef = Arc<dyn SQLExecutor>;

#[async_trait]
pub trait SQLExecutor: Sync + Send {
    // Context
    /// Executor name
    fn name(&self) -> &str;
    /// Executor compute context allows differentiating the remote compute context
    /// such as authorization or active database.
    fn compute_context(&self) -> Option<String>;

    // Execution
    /// Execute a SQL query
    fn execute(&self, query: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream>;

    // Schema inference
    /// Returns the tables provided by the remote
    async fn table_names(&self) -> Result<Vec<String>>;
    /// Returns the schema of table_name within this SQLExecutor
    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef>;
}

impl fmt::Debug for dyn SQLExecutor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {:?}", self.name(), self.compute_context())
    }
}

impl fmt::Display for dyn SQLExecutor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {:?}", self.name(), self.compute_context())
    }
}
