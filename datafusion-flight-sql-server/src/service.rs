use std::{pin::Pin, sync::Arc};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    datatypes::{DataType, Field, SchemaRef},
    error::ArrowError,
    ipc::writer::IpcWriteOptions,
};
use arrow_flight::sql::{
    self,
    server::{FlightSqlService as ArrowFlightSqlService, PeekableFlightDataStream},
    ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
    ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
    ActionEndSavepointRequest, ActionEndTransactionRequest, Any, CommandGetCatalogs,
    CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandGetXdbcTypeInfo, CommandPreparedStatementQuery, CommandPreparedStatementUpdate,
    CommandStatementQuery, CommandStatementSubstraitPlan, CommandStatementUpdate,
    DoPutPreparedStatementResult, ProstMessageExt as _, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    IpcMessage, SchemaAsIpc, Ticket,
};
use datafusion::{
    common::arrow::datatypes::Schema,
    dataframe::DataFrame,
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::{SQLOptions, SessionContext, SessionState},
    logical_expr::LogicalPlan,
    physical_plan::SendableRecordBatchStream,
};
use datafusion_substrait::{
    logical_plan::consumer::from_substrait_plan, serializer::deserialize_bytes,
};
use futures::{Stream, StreamExt, TryStreamExt};
use log::info;
use once_cell::sync::Lazy;
use prost::bytes::Bytes;
use prost::Message;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use super::session::{SessionStateProvider, StaticSessionStateProvider};
use super::state::{CommandTicket, QueryHandle};

type Result<T, E = Status> = std::result::Result<T, E>;

/// FlightSqlService is a basic stateless FlightSqlService implementation.
pub struct FlightSqlService {
    provider: Box<dyn SessionStateProvider>,
    sql_options: Option<SQLOptions>,
}

impl FlightSqlService {
    /// Creates a new FlightSqlService with a static SessionState.
    pub fn new(state: SessionState) -> Self {
        Self::new_with_provider(Box::new(StaticSessionStateProvider::new(state)))
    }

    /// Creates a new FlightSqlService with a SessionStateProvider.
    pub fn new_with_provider(provider: Box<dyn SessionStateProvider>) -> Self {
        Self {
            provider,
            sql_options: None,
        }
    }

    /// Replaces the sql_options with the provided options.
    /// These options are used to verify all SQL queries.
    /// When None the default [`SQLOptions`] are used.
    pub fn with_sql_options(self, sql_options: Option<SQLOptions>) -> Self {
        Self {
            sql_options,
            ..self
        }
    }

    // Federate substrait plans instead of SQL
    // pub fn with_substrait() -> Self {
    // TODO: Substrait federation
    // }

    // Serves straightforward on the specified address.
    pub async fn serve(self, addr: String) -> Result<(), Box<dyn std::error::Error>> {
        let addr = addr.parse()?;
        info!("Listening on {addr:?}");

        let svc = FlightServiceServer::new(self);

        Ok(Server::builder().add_service(svc).serve(addr).await?)
    }

    fn new_context<T>(&self, request: Request<T>) -> Result<(Request<T>, FlightSqlSessionContext)> {
        let (metadata, extensions, msg) = request.into_parts();
        let inspect_request = Request::from_parts(metadata, extensions, ());

        let state = self.provider.new_context(&inspect_request)?;
        let ctx = SessionContext::new_with_state(state);

        let (metadata, extensions, _) = inspect_request.into_parts();
        Ok((
            Request::from_parts(metadata, extensions, msg),
            FlightSqlSessionContext {
                inner: ctx,
                sql_options: self.sql_options,
            },
        ))
    }
}

/// The schema for GetTableTypes
static GET_TABLE_TYPES_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    //TODO: Move this into arrow-flight itself, similar to the builder pattern for CommandGetCatalogs and CommandGetDbSchemas
    Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]))
});

struct FlightSqlSessionContext {
    inner: SessionContext,
    sql_options: Option<SQLOptions>,
}

impl FlightSqlSessionContext {
    async fn sql_to_logical_plan(&self, sql: &str) -> DataFusionResult<LogicalPlan> {
        let plan = self.inner.state().create_logical_plan(sql).await?;
        let verifier = self.sql_options.unwrap_or_default();
        verifier.verify_plan(&plan)?;
        Ok(plan)
    }

    async fn execute_sql(&self, sql: &str) -> DataFusionResult<SendableRecordBatchStream> {
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
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse>> + Send>>>> {
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
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        let (request, ctx) = self.new_context(request)?;

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
            sql::Command::CommandStatementSubstraitPlan(CommandStatementSubstraitPlan {
                plan,
                ..
            }) => {
                let substrait_bytes = &plan
                    .ok_or(Status::invalid_argument(
                        "Expected substrait plan, found None",
                    ))?
                    .plan;

                let plan = parse_substrait_bytes(&ctx, substrait_bytes).await?;

                let state = ctx.inner.state();
                let df = DataFrame::new(state, plan);

                let stream = df.execute_stream().await.map_err(df_error_to_status)?;
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
    ) -> Result<Response<FlightInfo>> {
        let (request, ctx) = self.new_context(request)?;

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
        query: CommandStatementSubstraitPlan,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_substrait_plan");
        let (request, ctx) = self.new_context(request)?;

        let substrait_bytes = &query
            .plan
            .as_ref()
            .ok_or(Status::invalid_argument(
                "Expected substrait plan, found None",
            ))?
            .plan;

        let plan = parse_substrait_bytes(&ctx, substrait_bytes).await?;

        let flight_descriptor = request.into_inner();

        let dataset_schema = get_schema_for_plan(plan);

        // Form the response ticket (that the client will pass back to DoGet)
        let ticket = CommandTicket::new(sql::Command::CommandStatementSubstraitPlan(query))
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

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        let (request, ctx) = self.new_context(request)?;

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
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_catalogs");
        let (request, _ctx) = self.new_context(request)?;

        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(arrow_error_to_status)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_schemas");
        let (request, _ctx) = self.new_context(request)?;
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(arrow_error_to_status)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_tables");
        let (request, _ctx) = self.new_context(request)?;

        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(arrow_error_to_status)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_table_types");
        let (request, _ctx) = self.new_context(request)?;

        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&GET_TABLE_TYPES_SCHEMA)
            .map_err(arrow_error_to_status)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_sql_info");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement CommandGetSqlInfo"))
    }

    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_primary_keys");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented(
            "Implement get_flight_info_primary_keys",
        ))
    }

    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_exported_keys");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented(
            "Implement get_flight_info_exported_keys",
        ))
    }

    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_imported_keys");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented(
            "Implement get_flight_info_imported_keys",
        ))
    }

    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_cross_reference");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented(
            "Implement get_flight_info_cross_reference",
        ))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        info!("get_flight_info_xdbc_type_info");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented(
            "Implement get_flight_info_xdbc_type_info",
        ))
    }

    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_statement");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_get_statement"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_prepared_statement");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_get_prepared_statement"))
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_catalogs");
        let (_request, ctx) = self.new_context(request)?;
        let catalog_names = ctx.inner.catalog_names();

        let mut builder = query.into_builder();
        for catalog_name in &catalog_names {
            builder.append(catalog_name);
        }
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_schemas");
        let (_request, ctx) = self.new_context(request)?;
        let catalog_name = query.catalog.clone();
        // Append all schemas to builder, the builder handles applying the filters.
        let mut builder = query.into_builder();
        if let Some(catalog_name) = &catalog_name {
            if let Some(catalog) = ctx.inner.catalog(catalog_name) {
                for schema_name in &catalog.schema_names() {
                    builder.append(catalog_name, schema_name);
                }
            }
        };

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_tables");
        let (_request, ctx) = self.new_context(request)?;
        let catalog_name = query.catalog.clone();
        let mut builder = query.into_builder();
        // Append all schemas/tables to builder, the builder handles applying the filters.
        if let Some(catalog_name) = &catalog_name {
            if let Some(catalog) = ctx.inner.catalog(catalog_name) {
                for schema_name in &catalog.schema_names() {
                    if let Some(schema) = catalog.schema(schema_name) {
                        for table_name in &schema.table_names() {
                            if let Some(table) =
                                schema.table(table_name).await.map_err(df_error_to_status)?
                            {
                                builder
                                    .append(
                                        catalog_name,
                                        schema_name,
                                        table_name,
                                        table.table_type().to_string(),
                                        &table.schema(),
                                    )
                                    .map_err(flight_error_to_status)?;
                            }
                        }
                    }
                }
            }
        };

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_table_types");
        let (_, _) = self.new_context(request)?;

        // Report all variants of table types that datafusion uses.
        let table_types: ArrayRef = Arc::new(StringArray::from(
            vec![TableType::Base, TableType::View, TableType::Temporary]
                .into_iter()
                .map(|tt| tt.to_string())
                .collect::<Vec<String>>(),
        ));

        let batch = RecordBatch::try_from_iter(vec![("table_type", table_types)]).unwrap();

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(GET_TABLE_TYPES_SCHEMA.clone())
            .build(futures::stream::once(async { Ok(batch) }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_sql_info");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_get_sql_info"))
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_primary_keys");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_get_primary_keys"))
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_exported_keys");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_get_exported_keys"))
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_imported_keys");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_get_imported_keys"))
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_cross_reference");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_get_cross_reference"))
    }

    async fn do_get_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>> {
        info!("do_get_xdbc_type_info");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_get_xdbc_type_info"))
    }

    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_statement_update");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_put_statement_update"))
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        info!("do_put_prepared_statement_query");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented(
            "Implement do_put_prepared_statement_query",
        ))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _handle: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_prepared_statement_update");
        let (_, _) = self.new_context(request)?;

        // statements like "CREATE TABLE.." or "SET datafusion.nnn.." call this function
        // and we are required to return some row count here
        Ok(-1)
    }

    async fn do_put_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_prepared_statement_update");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented(
            "Implement do_put_prepared_statement_update",
        ))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let (_, ctx) = self.new_context(request)?;

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
        request: Request<Action>,
    ) -> Result<(), Status> {
        let (_, _) = self.new_context(request)?;

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
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        info!("do_action_create_prepared_substrait_plan");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented(
            "Implement do_action_create_prepared_substrait_plan",
        ))
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        let (_, _) = self.new_context(request)?;

        info!("do_action_begin_transaction");
        Err(Status::unimplemented(
            "Implement do_action_begin_transaction",
        ))
    }

    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        info!("do_action_end_transaction");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_action_end_transaction"))
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        info!("do_action_begin_savepoint");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_action_begin_savepoint"))
    }

    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        info!("do_action_end_savepoint");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_action_end_savepoint"))
    }

    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        info!("do_action_cancel_query");
        let (_, _) = self.new_context(request)?;

        Err(Status::unimplemented("Implement do_action_cancel_query"))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Takes a substrait plan serialized as [Bytes] and deserializes this to
/// a Datafusion [LogicalPlan]
async fn parse_substrait_bytes(
    ctx: &FlightSqlSessionContext,
    substrait: &Bytes,
) -> Result<LogicalPlan> {
    let substrait_plan = deserialize_bytes(substrait.to_vec())
        .await
        .map_err(df_error_to_status)?;

    from_substrait_plan(&ctx.inner, &substrait_plan)
        .await
        .map_err(df_error_to_status)
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
