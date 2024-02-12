use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::{
    FlightSqlService as ArrowFlightSqlService, PeekableFlightDataStream,
};
use arrow_flight::sql::{
    self, ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
    ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
    ActionEndSavepointRequest, ActionEndTransactionRequest, Any, CommandGetCatalogs,
    CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandGetXdbcTypeInfo, CommandPreparedStatementQuery, CommandPreparedStatementUpdate,
    CommandStatementQuery, CommandStatementSubstraitPlan, CommandStatementUpdate, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    IpcMessage, SchemaAsIpc, Ticket,
};
use datafusion::common::arrow::datatypes::Schema;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{SQLOptions, SessionContext, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt, TryStreamExt};
use log::info;
use prost::bytes::Bytes;
use std::pin::Pin;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use super::state::{CommandTicket, QueryHandle};

// FlightSqlService is a basic stateless FlightSqlService implementation.
pub struct FlightSqlService {
    session_state: SessionState,
}

impl FlightSqlService {
    pub fn new_sql(state: SessionState) -> Self {
        Self {
            session_state: state,
        }
    }

    pub async fn run_sql(addr: String, state: SessionState) -> Result<()> {
        let addr = addr
            .parse()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let service = Self::new_sql(state);
        info!("Listening on {addr:?}");

        let svc = FlightServiceServer::new(service);

        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
    }

    fn new_context(&self) -> FlightSqlSessionContext {
        let ctx = SessionContext::new_with_state(self.session_state.clone());

        FlightSqlSessionContext { inner: ctx }
    }
}

struct FlightSqlSessionContext {
    inner: SessionContext,
}

impl FlightSqlSessionContext {
    async fn sql_to_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let plan = self.inner.state().create_logical_plan(sql).await?;
        let verifier = SQLOptions::new();
        verifier.verify_plan(&plan)?;
        Ok(plan)
    }

    async fn execute_sql(&self, sql: &str) -> Result<SendableRecordBatchStream> {
        let plan = self.sql_to_logical_plan(sql).await?;
        self.inner
            .execute_logical_plan(plan)
            .await?
            .execute_stream()
            .await
    }
}

#[tonic::async_trait]
impl ArrowFlightSqlService for FlightSqlService {
    type FlightService = FlightSqlService;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        info!("do_handshake");
        // Favor middleware over handshake
        // https://github.com/apache/arrow/issues/23836
        // https://github.com/apache/arrow/issues/25848
        Err(Status::unimplemented("handshake is not supported"))
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ctx = self.new_context();

        let ticket = CommandTicket::try_decode(request.into_inner().ticket)
            .map_err(flight_error_to_status)?;

        match ticket.command {
            sql::Command::CommandStatementQuery(CommandStatementQuery { query, .. }) => {
                // print!("Query: {query}\n");

                let stream = ctx.execute_sql(&query).await.map_err(df_error_to_status)?;
                let arrow_schema = stream.schema();
                let arrow_stream = stream.map(|i| {
                    let batch = i.map_err(|e| FlightError::ExternalError(e.into()))?;
                    Ok(batch)
                });

                let flight_data_stream = FlightDataEncoderBuilder::new()
                    .with_schema(arrow_schema)
                    .build(arrow_stream)
                    .map_err(flight_error_to_status)
                    .boxed();

                Ok(Response::new(flight_data_stream))
            }
            sql::Command::CommandPreparedStatementQuery(CommandPreparedStatementQuery {
                prepared_statement_handle,
            }) => {
                let query = std::str::from_utf8(prepared_statement_handle.as_ref()).unwrap();
                // print!("Query: {query}\n");

                let stream = ctx.execute_sql(query).await.map_err(df_error_to_status)?;
                let arrow_schema = stream.schema();
                let arrow_stream = stream.map(|i| {
                    let batch = i.map_err(|e| FlightError::ExternalError(e.into()))?;
                    Ok(batch)
                });

                let flight_data_stream = FlightDataEncoderBuilder::new()
                    .with_schema(arrow_schema)
                    .build(arrow_stream)
                    .map_err(flight_error_to_status)
                    .boxed();

                Ok(Response::new(flight_data_stream))
            }
            _ => {
                return Err(Status::internal(format!(
                    "statement handle not found: {:?}",
                    ticket.command
                )));
            }
        }
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ctx = self.new_context();

        let sql = &query.query;
        info!("get_flight_info_statement with query={sql}");

        let flight_descriptor = request.into_inner();

        let plan = ctx
            .sql_to_logical_plan(sql)
            .await
            .map_err(df_error_to_status)?;

        let dataset_schema = get_schema_for_plan(plan);

        // Form the response ticket (that the client will pass back to DoGet)
        let ticket = CommandTicket::new(sql::Command::CommandStatementQuery(query))
            .try_encode()
            .map_err(flight_error_to_status)?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket { ticket });

        let flight_info = FlightInfo::new()
            .with_endpoint(endpoint)
            // return descriptor we were passed
            .with_descriptor(flight_descriptor)
            .try_with_schema(dataset_schema.as_ref())
            .map_err(arrow_error_to_status)?;

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_substrait_plan");
        let _ctx = self.new_context();

        Err(Status::unimplemented(
            "Implement get_flight_info_substrait_plan",
        ))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ctx = self.new_context();

        let handle = QueryHandle::try_decode(cmd.prepared_statement_handle.clone())
            .map_err(|e| Status::internal(format!("Error decoding handle: {e}")))?;

        info!("get_flight_info_prepared_statement with handle={handle}");

        let flight_descriptor = request.into_inner();

        let sql = handle.query();
        let plan = ctx
            .sql_to_logical_plan(sql)
            .await
            .map_err(df_error_to_status)?;

        let dataset_schema = get_schema_for_plan(plan);

        // Form the response ticket (that the client will pass back to DoGet)
        let ticket = CommandTicket::new(sql::Command::CommandPreparedStatementQuery(cmd))
            .try_encode()
            .map_err(flight_error_to_status)?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket { ticket });

        let flight_info = FlightInfo::new()
            .with_endpoint(endpoint)
            // return descriptor we were passed
            .with_descriptor(flight_descriptor)
            .try_with_schema(dataset_schema.as_ref())
            .map_err(arrow_error_to_status)?;

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_catalogs");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement get_flight_info_catalogs"))
    }

    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_schemas");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement get_flight_info_schemas"))
    }

    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_tables");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement get_flight_info_tables"))
    }

    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_table_types");
        let _ctx = self.new_context();

        Err(Status::unimplemented(
            "Implement get_flight_info_table_types",
        ))
    }

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_sql_info");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement CommandGetSqlInfo"))
    }

    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_primary_keys");
        let _ctx = self.new_context();

        Err(Status::unimplemented(
            "Implement get_flight_info_primary_keys",
        ))
    }

    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_exported_keys");
        let _ctx = self.new_context();

        Err(Status::unimplemented(
            "Implement get_flight_info_exported_keys",
        ))
    }

    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_imported_keys");
        let _ctx = self.new_context();

        Err(Status::unimplemented(
            "Implement get_flight_info_imported_keys",
        ))
    }

    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_cross_reference");
        let _ctx = self.new_context();

        Err(Status::unimplemented(
            "Implement get_flight_info_cross_reference",
        ))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_xdbc_type_info");
        let _ctx = self.new_context();

        Err(Status::unimplemented(
            "Implement get_flight_info_xdbc_type_info",
        ))
    }

    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_statement");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_get_statement"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_prepared_statement");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_get_prepared_statement"))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_catalogs");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_get_catalogs"))
    }

    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_schemas");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_get_schemas"))
    }

    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_tables");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_get_tables"))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_table_types");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_get_table_types"))
    }

    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_sql_info");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_get_sql_info"))
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_primary_keys");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_get_primary_keys"))
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_exported_keys");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_get_exported_keys"))
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_imported_keys");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_get_imported_keys"))
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_cross_reference");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_get_cross_reference"))
    }

    async fn do_get_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_xdbc_type_info");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_get_xdbc_type_info"))
    }

    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_statement_update");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_put_statement_update"))
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        info!("do_put_prepared_statement_query");
        let _ctx = self.new_context();

        Err(Status::unimplemented(
            "Implement do_put_prepared_statement_query",
        ))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _handle: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_prepared_statement_update");
        let _ctx = self.new_context();

        // statements like "CREATE TABLE.." or "SET datafusion.nnn.." call this function
        // and we are required to return some row count here
        Ok(-1)
    }

    async fn do_put_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_prepared_statement_update");
        let _ctx = self.new_context();

        Err(Status::unimplemented(
            "Implement do_put_prepared_statement_update",
        ))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let ctx = self.new_context();

        let sql = query.query.clone();
        info!(
            "do_action_create_prepared_statement query={:?}",
            query.query
        );

        let plan = ctx
            .sql_to_logical_plan(sql.as_str())
            .await
            .map_err(df_error_to_status)?;

        let dataset_schema = get_schema_for_plan(plan);

        let schema_bytes = encode_schema(dataset_schema.as_ref()).map_err(arrow_error_to_status)?;
        let handle = QueryHandle::new(sql);

        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: Bytes::from(handle),
            dataset_schema: schema_bytes,
            parameter_schema: Default::default(),
        };

        Ok(res)
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        let _ctx = self.new_context();

        let handle = query.prepared_statement_handle.as_ref();
        if let Ok(handle) = std::str::from_utf8(handle) {
            info!(
                "do_action_close_prepared_statement with handle {:?}",
                handle
            );

            // NOP since stateless
        }
        Ok(())
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        info!("do_action_create_prepared_substrait_plan");
        let _ctx = self.new_context();

        Err(Status::unimplemented(
            "Implement do_action_create_prepared_substrait_plan",
        ))
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        let _ctx = self.new_context();

        info!("do_action_begin_transaction");
        Err(Status::unimplemented(
            "Implement do_action_begin_transaction",
        ))
    }

    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        info!("do_action_end_transaction");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_action_end_transaction"))
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        info!("do_action_begin_savepoint");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_action_begin_savepoint"))
    }

    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        info!("do_action_end_savepoint");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_action_end_savepoint"))
    }

    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        info!("do_action_cancel_query");
        let _ctx = self.new_context();

        Err(Status::unimplemented("Implement do_action_cancel_query"))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Encodes the schema IPC encoded (schema_bytes)
fn encode_schema(schema: &Schema) -> std::result::Result<Bytes, ArrowError> {
    let options = IpcWriteOptions::default();

    // encode the schema into the correct form
    let message: Result<IpcMessage, ArrowError> = SchemaAsIpc::new(schema, &options).try_into();

    let IpcMessage(schema) = message?;

    Ok(schema)
}

/// Return the schema for the specified logical plan
fn get_schema_for_plan(logical_plan: LogicalPlan) -> SchemaRef {
    // gather real schema, but only
    let schema = Arc::new(Schema::from(logical_plan.schema().as_ref())) as _;

    schema
}

fn arrow_error_to_status(err: ArrowError) -> Status {
    Status::internal(format!("{err:?}"))
}

fn flight_error_to_status(err: FlightError) -> Status {
    Status::internal(format!("{err:?}"))
}

fn df_error_to_status(err: DataFusionError) -> Status {
    Status::internal(format!("{err:?}"))
}
