use thiserror::Error;

/// Errors produced by the storage agent.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AgentError {
    #[error("zenoh error: {0}")]
    Zenoh(#[from] zenoh::Error),

    #[error("store error: {0}")]
    Store(String),

    #[error("recovery error: {0}")]
    Recovery(String),

    #[error("invalid configuration: {0}")]
    Config(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("mitiflow error: {0}")]
    Mitiflow(#[from] mitiflow::Error),
}

/// Convenience result type for the storage agent.
pub type AgentResult<T> = std::result::Result<T, AgentError>;
