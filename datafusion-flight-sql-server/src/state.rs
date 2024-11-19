use std::fmt::Display;

use arrow_flight::{
    error::FlightError,
    sql::{self, Any, Command},
};
use prost::{bytes::Bytes, Message};

pub type Result<T, E = FlightError> = std::result::Result<T, E>;

#[derive(Debug, PartialEq, Clone)]
pub struct CommandTicket {
    pub command: sql::Command,
}

impl CommandTicket {
    pub fn new(cmd: sql::Command) -> Self {
        Self { command: cmd }
    }

    pub fn try_decode(msg: Bytes) -> Result<Self> {
        let msg = CommandTicketMessage::decode(msg).map_err(decode_error_flight_error)?;

        Self::try_decode_command(msg.command)
    }

    pub fn try_decode_command(cmd: Bytes) -> Result<Self> {
        let content_msg = Any::decode(cmd).map_err(decode_error_flight_error)?;
        let command = Command::try_from(content_msg).map_err(FlightError::Arrow)?;

        Ok(Self { command })
    }

    pub fn try_encode(self) -> Result<Bytes> {
        let content_msg = self.command.into_any().encode_to_vec();

        let msg = CommandTicketMessage {
            command: content_msg.into(),
        };

        Ok(msg.encode_to_vec().into())
    }
}

#[derive(Clone, PartialEq, Message)]
struct CommandTicketMessage {
    #[prost(bytes = "bytes", tag = "2")]
    command: Bytes,
}

fn decode_error_flight_error(err: prost::DecodeError) -> FlightError {
    FlightError::DecodeError(format!("{err:?}"))
}

/// Represents a query handle for use in prepared statements.
/// All state required to run the prepared statement is passed
/// back and forth to the client, so any service instance can run it
#[derive(Debug, Clone)]
pub struct QueryHandle {
    /// The raw SQL query text
    query: String,
    parameters: Option<Bytes>,
}

impl QueryHandle {
    pub fn new(query: String, parameters: Option<Bytes>) -> Self {
        Self { query, parameters }
    }

    pub fn query(&self) -> &str {
        self.query.as_ref()
    }

    pub fn parameters(&self) -> Option<&[u8]> {
        self.parameters.as_deref()
    }

    pub fn set_parameters(&mut self, parameters: Option<Bytes>) {
        self.parameters = parameters;
    }

    pub fn try_decode(msg: Bytes) -> Result<Self> {
        let msg = QueryHandleMessage::decode(msg).map_err(decode_error_flight_error)?;

        Ok(Self {
            query: msg.query,
            parameters: msg.parameters,
        })
    }

    pub fn encode(self) -> Bytes {
        let msg = QueryHandleMessage {
            query: self.query,
            parameters: self.parameters,
        };

        msg.encode_to_vec().into()
    }
}

impl From<QueryHandle> for Bytes {
    fn from(value: QueryHandle) -> Self {
        value.encode()
    }
}

impl Display for QueryHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Query({})", self.query)
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct QueryHandleMessage {
    /// The raw SQL query text
    #[prost(string, tag = "1")]
    query: String,
    #[prost(bytes = "bytes", optional, tag = "2")]
    parameters: Option<Bytes>,
}
