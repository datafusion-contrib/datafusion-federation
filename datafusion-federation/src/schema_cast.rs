use async_stream::stream;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        ExecutionPlanProperties, PlanProperties,
    },
};
use futures::StreamExt;
use std::any::Any;
use std::clone::Clone;
use std::fmt;
use std::sync::Arc;

mod intervals_cast;
mod lists_cast;
pub mod record_convert;
mod struct_cast;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct SchemaCastScanExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl SchemaCastScanExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        let eq_properties = input.equivalence_properties().clone();
        let execution_mode = input.execution_mode();
        let properties = PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            execution_mode,
        );
        Self {
            input,
            schema,
            properties,
        }
    }
}

impl DisplayAs for SchemaCastScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SchemaCastScanExec")
    }
}

impl ExecutionPlan for SchemaCastScanExec {
    fn name(&self) -> &str {
        "SchemaCastScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(Self::new(
                Arc::clone(&children[0]),
                Arc::clone(&self.schema),
            )))
        } else {
            Err(DataFusionError::Execution(
                "SchemaCastScanExec expects exactly one input".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut stream = self.input.execute(partition, context)?;
        let schema = Arc::clone(&self.schema);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            {
                stream! {
                    while let Some(batch) = stream.next().await {
                        let batch = record_convert::try_cast_to(batch?, Arc::clone(&schema));
                        yield batch.map_err(|e| { DataFusionError::External(Box::new(e)) });
                    }
                }
            },
        )))
    }
}
