use arrow::{datatypes::Schema, error::ArrowError};
use arrow_flight::{sql::client::FlightSqlServiceClient, FlightInfo};
use async_trait::async_trait;
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
};
use datafusion_federation_sql::SQLExecutor;
use futures::{executor, TryStreamExt};

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
    mut client: FlightSqlServiceClient<Channel>,
    flight_info: FlightInfo,
    schema: Arc<Schema>,
) -> Result<SendableRecordBatchStream> {
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
    fn execute(&self, sql: &str) -> Result<SendableRecordBatchStream> {
        let mut client = self.client.clone();
        let flight_info =
            executor::block_on(async move { client.execute(sql.to_string(), None).await })
                .map_err(arrow_error_to_df)?;

        let schema = Arc::new(
            flight_info
                .clone()
                .try_decode_schema()
                .map_err(arrow_error_to_df)?,
        );

        let future_stream =
            make_flight_sql_stream(self.client.clone(), flight_info, schema.clone());
        let stream = futures::stream::once(future_stream).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn arrow_error_to_df(err: ArrowError) -> DataFusionError {
    DataFusionError::External(format!("arrow error: {err:?}").into())
}
