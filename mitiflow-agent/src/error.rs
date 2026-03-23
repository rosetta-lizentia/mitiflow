use miette::Diagnostic;
use thiserror::Error;

/// Errors produced by the storage agent.
#[derive(Debug, Error, Diagnostic)]
#[non_exhaustive]
pub enum AgentError {
    #[error("zenoh error: {0}")]
    #[diagnostic(help("Check Zenoh connectivity. Ensure the Zenoh router or peer is reachable."))]
    Zenoh(#[from] zenoh::Error),

    #[error("store error: {0}")]
    #[diagnostic(
        code(mitiflow::agent::store),
        help("Check disk space and permissions for the data directory. Another process may be locking the store.")
    )]
    Store(String),

    #[error("recovery error: {0}")]
    #[diagnostic(help("Recovery from peers failed. Check that other agents are online and reachable."))]
    Recovery(String),

    #[error("invalid configuration: {0}")]
    #[diagnostic(code(mitiflow::agent::config), help("Check the agent configuration file or environment variables."))]
    Config(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("mitiflow error: {0}")]
    Mitiflow(#[from] mitiflow::Error),
}

/// Convenience result type for the storage agent.
pub type AgentResult<T> = std::result::Result<T, AgentError>;

#[cfg(test)]
mod tests {
    use super::*;
    use miette::Diagnostic;

    #[test]
    fn store_error_has_diagnostic_help() {
        let err = AgentError::Store("partition locked".into());
        let help = err.help().map(|h| h.to_string());
        assert!(help.is_some());
        assert!(help.unwrap().contains("disk space"));
    }

    #[test]
    fn store_error_has_code() {
        let err = AgentError::Store("locked".into());
        let code = err.code().map(|c| c.to_string());
        assert_eq!(code.as_deref(), Some("mitiflow::agent::store"));
    }

    #[test]
    fn config_error_has_help() {
        let err = AgentError::Config("missing field".into());
        let help = err.help().map(|h| h.to_string());
        assert!(help.is_some());
        assert!(help.unwrap().contains("configuration"));
    }

    #[test]
    fn recovery_error_has_help() {
        let err = AgentError::Recovery("peer timeout".into());
        let help = err.help().map(|h| h.to_string());
        assert!(help.is_some());
        assert!(help.unwrap().contains("agents"));
    }
}
