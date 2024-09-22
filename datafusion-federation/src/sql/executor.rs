use async_trait::async_trait;
use core::fmt;
use datafusion::{
    arrow::datatypes::SchemaRef, error::Result, physical_plan::SendableRecordBatchStream,
    sql::sqlparser::ast, sql::unparser::dialect::Dialect,
};
use std::sync::Arc;

pub type SQLExecutorRef = Arc<dyn SQLExecutor>;
pub type AstAnalyzer = Box<dyn Fn(ast::Statement) -> Result<ast::Statement>>;

#[async_trait]
pub trait SQLExecutor: Sync + Send {
    /// Executor name
    fn name(&self) -> &str;

    /// Executor compute context allows differentiating the remote compute context
    /// such as authorization or active database.
    ///
    /// Note: returning None here may cause incorrect federation with other providers of the
    /// same name that also have a compute_context of None.
    /// Instead try to return a unique string that will never match any other
    /// provider's context.
    fn compute_context(&self) -> Option<String>;

    /// The specific SQL dialect (currently supports 'sqlite', 'postgres', 'flight')
    fn dialect(&self) -> Arc<dyn Dialect>;

    /// Returns an AST analyzer specific for this engine to modify the AST before execution
    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        None
    }

    /// Execute a SQL query
    fn execute(&self, query: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream>;

    /// Returns the tables provided by the remote
    async fn table_names(&self) -> Result<Vec<String>>;

    /// Returns the schema of table_name within this [`SQLExecutor`]
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
