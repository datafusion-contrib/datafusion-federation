use async_trait::async_trait;
use connectorx::{
    destinations::arrow::ArrowDestinationError,
    errors::{ConnectorXError, ConnectorXOutError},
    prelude::{get_arrow, CXQuery, SourceConn},
};
use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    error::{DataFusionError, Result},
    physical_plan::{
        stream::RecordBatchStreamAdapter, EmptyRecordBatchStream, SendableRecordBatchStream,
    },
};
use std::sync::Arc;

use crate::executor::SQLExecutor;

pub struct CXExecutor {
    context: String,
    conn: SourceConn,
}

impl CXExecutor {
    pub fn new(dsn: String) -> Result<Self> {
        let conn = SourceConn::try_from(dsn.as_str()).map_err(cx_error_to_df)?;
        Ok(Self { context: dsn, conn })
    }

    pub fn new_with_conn(conn: SourceConn) -> Self {
        Self {
            context: conn.conn.to_string(),
            conn,
        }
    }

    pub fn context(&mut self, context: String) {
        self.context = context;
    }
}

fn cx_error_to_df(err: ConnectorXError) -> DataFusionError {
    DataFusionError::External(format!("ConnectorX: {err:?}").into())
}

#[async_trait]
impl SQLExecutor for CXExecutor {
    fn name(&self) -> &str {
        "connector_x_executor"
    }
    fn compute_context(&self) -> Option<String> {
        Some(self.context.clone())
    }
    fn execute(&self, sql: &str, _schema: SchemaRef) -> Result<SendableRecordBatchStream> {
        let conn = self.conn.clone();
        let query: CXQuery = sql.into();

        let mut dst = get_arrow(&conn, None, &[query.clone()]).map_err(cx_out_error_to_df)?;
        let stream = if let Some(batch) = dst.record_batch().map_err(cx_dst_error_to_df)? {
            futures::stream::once(async move { Ok(batch) })
        } else {
            return Ok(Box::pin(EmptyRecordBatchStream::new(Arc::new(
                Schema::empty(),
            ))));
        };

        let schema = schema_to_lowercase(dst.arrow_schema());

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "connector_x source: table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        let conn = self.conn.clone();
        let query: CXQuery = format!("select * from {table_name} limit 1")
            .as_str()
            .into();

        let dst = get_arrow(&conn, None, &[query.clone()]).map_err(cx_out_error_to_df)?;
        let schema = schema_to_lowercase(dst.arrow_schema());
        Ok(schema)
    }
}

fn cx_dst_error_to_df(err: ArrowDestinationError) -> DataFusionError {
    DataFusionError::External(format!("ConnectorX failed to run query: {err:?}").into())
}

/// Get the schema with lowercase field names
fn schema_to_lowercase(schema: SchemaRef) -> SchemaRef {
    // DF needs lower case schema
    let lower_fields: Vec<_> = schema
        .fields
        .iter()
        .map(|f| {
            Field::new(
                f.name().to_ascii_lowercase(),
                f.data_type().clone(),
                f.is_nullable(),
            )
        })
        .collect();

    Arc::new(Schema::new(lower_fields))
}

fn cx_out_error_to_df(err: ConnectorXOutError) -> DataFusionError {
    DataFusionError::External(format!("ConnectorX failed to run query: {err:?}").into())
}
