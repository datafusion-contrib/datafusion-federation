use arrow::error::ArrowError;
use arrow_flight::{sql::client::FlightSqlServiceClient, Ticket};
use async_trait::async_trait;
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
};
use datafusion_federation_sql::SQLExecutor;
use futures::{executor, StreamExt};
use std::sync::Arc;
use tonic::transport::Endpoint;

pub struct FlightSQLExecutor {
    context: String,
}

impl FlightSQLExecutor {
    pub fn new(dsn: String) -> Self {
        Self { context: dsn }
    }

    pub fn context(&mut self, context: String) {
        self.context = context;
    }
}

async fn new_client(dns: String) -> Result<FlightSqlServiceClient<tonic::transport::Channel>> {
    let endpoint = Endpoint::new(dns.clone()).map_err(tx_error_to_df)?;
    let channel = endpoint.connect().await.map_err(tx_error_to_df)?;

    Ok(FlightSqlServiceClient::new(channel))
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
        let dsn = self.context.clone();
        let flight_info = executor::block_on(async move {
            let client = &mut new_client(dsn).await?;
            client.execute(sql.to_string(), None).await
        })
        .map_err(arrow_error_to_df)?;
        let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap().clone();
        let schema = flight_info.try_decode_schema().map_err(arrow_error_to_df)?;

        let stream = futures::stream::try_unfold(
            (
                None,
                Some(InitStream {
                    dsn: self.context.clone(),
                    ticket,
                }),
            ),
            |(maybe_stream, maybe_init)| async move {
                let mut stream = match maybe_stream {
                    Some(stream) => stream,
                    None => {
                        let init = maybe_init.unwrap();
                        let client = &mut new_client(init.dsn).await?;
                        client
                            .do_get(init.ticket)
                            .await
                            .map_err(arrow_error_to_df)?
                    }
                };
                let maybe_batch = stream
                    .next()
                    .await
                    .transpose()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                match maybe_batch {
                    Some(batch) => Ok::<_, DataFusionError>(Some((batch, (Some(stream), None)))),
                    None => Ok(None),
                }
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(schema),
            stream,
        )))
    }
}

struct InitStream {
    dsn: String,
    ticket: Ticket,
}

fn tx_error_to_df(err: tonic::transport::Error) -> DataFusionError {
    DataFusionError::External(format!("failed to connect: {err:?}").into())
}

fn arrow_error_to_df(err: ArrowError) -> DataFusionError {
    DataFusionError::External(format!("arrow error: {err:?}").into())
}
