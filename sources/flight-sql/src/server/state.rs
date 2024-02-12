use arrow_flight::{
    error::FlightError,
    sql::{self, Any, Command},
};
use prost::{bytes::Bytes, Message};
use std::fmt::Display;

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
#[derive(Debug, Clone, PartialEq)]
pub struct QueryHandle {
    /// The raw SQL query text
    query: String,
}

impl QueryHandle {
    pub fn new(query: String) -> Self {
        Self { query }
    }

    pub fn query(&self) -> &str {
        self.query.as_ref()
    }

    pub fn try_decode(handle: Bytes) -> Result<Self> {
        let query = String::from_utf8(handle.to_vec())
            .map_err(|e| FlightError::ExternalError(Box::new(e.utf8_error())))?;
        Ok(Self { query })
    }

    pub fn encode(self) -> Bytes {
        Bytes::from(self.query.into_bytes())
    }
}

impl Display for QueryHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Query({})", self.query)
    }
}

/// Encode a QueryHandle as Bytes
impl From<QueryHandle> for Bytes {
    fn from(value: QueryHandle) -> Self {
        Self::from(value.query.into_bytes())
    }
}
