use datafusion::execution::SessionState;
use tonic::{Request, Status};

type Result<T, E = Status> = std::result::Result<T, E>;

// SessionStateProvider is a trait used to provide a SessionState for a given
// request.
pub trait SessionStateProvider: Sync + Send {
    fn new_context(&self, request: &Request<()>) -> Result<SessionState>;
}

// StaticSessionStateProvider is a simple implementation of SessionStateProvider that
// uses a static SessionState.
pub(crate) struct StaticSessionStateProvider {
    state: SessionState,
}

impl StaticSessionStateProvider {
    pub fn new(state: SessionState) -> Self {
        Self { state }
    }
}

impl SessionStateProvider for StaticSessionStateProvider {
    fn new_context(&self, _request: &Request<()>) -> Result<SessionState> {
        Ok(self.state.clone())
    }
}
