use arrow::{datatypes::SchemaRef, error::ArrowError};
use arrow_flight::sql::client::FlightSqlServiceClient;
use async_trait::async_trait;
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
};
use datafusion_federation_sql::SQLExecutor;
use futures::TryStreamExt;

use std::sync::Arc;
use tonic::transport::Channel;

pub struct FlightSQLExecutor {
    context: String,
    client: FlightSqlServiceClient<Channel>,
}

impl FlightSQLExecutor {
    pub fn new(dsn: String, client: FlightSqlServiceClient<Channel>) -> Self {
        Self {
            context: dsn,
            client,
        }
    }

    pub fn context(&mut self, context: String) {
        self.context = context;
    }
}

async fn make_flight_sql_stream(
    sql: String,
    mut client: FlightSqlServiceClient<Channel>,
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    let flight_info = client
        .execute(sql.to_string(), None)
        .await
        .map_err(arrow_error_to_df)?;

    let mut flight_data_streams = Vec::with_capacity(flight_info.endpoint.len());
    for endpoint in flight_info.endpoint {
        let ticket = endpoint.ticket.ok_or(DataFusionError::Execution(
            "FlightEndpoint missing ticket!".to_string(),
        ))?;
        let flight_data = client.do_get(ticket).await?;
        flight_data_streams.push(flight_data);
    }

    let record_batch_stream = futures::stream::select_all(flight_data_streams)
        .map_err(|e| DataFusionError::External(Box::new(e)));

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        record_batch_stream,
    )))
}

#[async_trait]
impl SQLExecutor for FlightSQLExecutor {
    fn name(&self) -> &str {
        "flight_sql_executor"
    }
    fn compute_context(&self) -> Option<String> {
        Some(self.context.clone())
    }
    fn execute(&self, sql: &str, schema: SchemaRef) -> Result<SendableRecordBatchStream> {
        let future_stream =
            make_flight_sql_stream(sql.to_string(), self.client.clone(), schema.clone());
        let stream = futures::stream::once(future_stream).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream,
        )))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "flight_sql source: table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        let sql = format!("select * from {table_name} limit 1");
        let flight_info = self
            .client
            .clone()
            .execute(sql, None)
            .await
            .map_err(arrow_error_to_df)?;
        let schema = flight_info.try_decode_schema().map_err(arrow_error_to_df)?;
        Ok(Arc::new(schema))
    }
}

fn arrow_error_to_df(err: ArrowError) -> DataFusionError {
    DataFusionError::External(format!("arrow error: {err:?}").into())
}
