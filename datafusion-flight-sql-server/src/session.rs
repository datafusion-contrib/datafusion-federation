use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use tonic::{Request, Status};

type Result<T, E = Status> = std::result::Result<T, E>;

// SessionStateProvider is a trait used to provide a SessionState for a given
// request.
#[async_trait]
pub trait SessionStateProvider: Sync + Send {
    async fn new_context(&self, request: &Request<()>) -> Result<SessionState>;
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

#[async_trait]
impl SessionStateProvider for StaticSessionStateProvider {
    async fn new_context(&self, _request: &Request<()>) -> Result<SessionState> {
        Ok(self.state.clone())
    }
}
