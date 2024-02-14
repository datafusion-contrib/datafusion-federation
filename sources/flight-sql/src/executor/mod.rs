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
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

/// Authentication options for FlightSQL Endpoints
#[derive(Clone)]
pub enum FlightSQLAuth {
    Basic(BasicAuth),
    PKI(PKIAuth),
    Unsecured,
}

/// Basic username and password authorization for FlightSQL Endpoints
#[derive(Clone)]
pub struct BasicAuth {
    username: String,
    password: String,
}

/// Contains options for completing mTLS/PKI authentication configuration of Tonic client
#[derive(Clone)]
pub struct PKIAuth {
    /// The public x509 cert pem file used to auth with the FlightSQL server
    pub client_cert_file: String,
    /// The private key pem file used to validate the public client cert
    pub client_key_file: String,
    /// The bundle of trusted CAcerts for validating the FlightSQL server
    pub ca_cert_bundle: String,
}

pub struct FlightSQLExecutor {
    context: String,
    auth: FlightSQLAuth,
}

impl FlightSQLExecutor {
    pub fn new(dns: String, auth: FlightSQLAuth) -> Self {
        Self { context: dns, auth }
    }

    pub fn context(&mut self, context: String) {
        self.context = context;
    }
}

/// Creates a new [FlightSqlServiceClient] for the passed endpoint. Completes the relevant auth configurations
/// or handshake as appropriate for the passed [FlightSQLAuth] variant.
async fn new_client(
    dns: String,
    auth: FlightSQLAuth,
) -> Result<FlightSqlServiceClient<tonic::transport::Channel>> {
    let endpoint = Endpoint::new(dns).map_err(tx_error_to_df)?;

    match auth {
        FlightSQLAuth::Basic(basic) => {
            let channel = endpoint.connect().await.map_err(tx_error_to_df)?;
            let mut client = FlightSqlServiceClient::new(channel);
            client
                .handshake(basic.username.as_str(), basic.password.as_str())
                .await?;
            Ok(client)
        }
        FlightSQLAuth::PKI(pki) => {
            let endpoint = endpoint
                .tls_config(
                    ClientTlsConfig::new()
                        .identity(Identity::from_pem(
                            pki.client_cert_file.as_str(),
                            pki.client_key_file.as_str(),
                        ))
                        .ca_certificate(Certificate::from_pem(pki.ca_cert_bundle.as_str())),
                )
                .map_err(tx_error_to_df)?;
            let channel = endpoint.connect().await.map_err(tx_error_to_df)?;
            Ok(FlightSqlServiceClient::new(channel))
        }
        FlightSQLAuth::Unsecured => {
            let channel = endpoint.connect().await.map_err(tx_error_to_df)?;
            Ok(FlightSqlServiceClient::new(channel))
        }
    }
}

async fn make_flight_sql_stream(
    dns: String,
    auth: FlightSQLAuth,
    flight_info: FlightInfo,
    schema: Arc<Schema>,
) -> Result<SendableRecordBatchStream> {
    let client = &mut new_client(dns, auth).await?;
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
        let dns = self.context.clone();
        let auth = self.auth.clone();
        let flight_info = executor::block_on(async move {
            let client = &mut new_client(dns, auth.clone()).await?;
            client.execute(sql.to_string(), None).await
        })
        .map_err(arrow_error_to_df)?;

        let schema = Arc::new(
            flight_info
                .clone()
                .try_decode_schema()
                .map_err(arrow_error_to_df)?,
        );

        let future_stream = make_flight_sql_stream(
            self.context.clone(),
            self.auth.clone(),
            flight_info,
            schema.clone(),
        );
        let stream = futures::stream::once(future_stream).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn tx_error_to_df(err: tonic::transport::Error) -> DataFusionError {
    DataFusionError::External(format!("failed to connect: {err:?}").into())
}

fn arrow_error_to_df(err: ArrowError) -> DataFusionError {
    DataFusionError::External(format!("arrow error: {err:?}").into())
}
