use async_trait::async_trait;
use core::fmt;
use datafusion::{error::Result, physical_plan::SendableRecordBatchStream};
use std::sync::Arc;

pub type SQLExecutorRef = Arc<dyn SQLExecutor>;

#[async_trait]
pub trait SQLExecutor: Sync + Send {
    fn name(&self) -> &str;
    fn compute_context(&self) -> Option<String>;
    fn execute(&self, query: &str) -> Result<SendableRecordBatchStream>;
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
