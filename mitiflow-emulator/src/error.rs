//! Error types for the mitiflow emulator.

use std::path::PathBuf;

/// Errors that can occur during emulator operation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EmulatorError {
    /// YAML configuration parsing error.
    #[error("config error: {0}")]
    Config(String),

    /// Topology validation error.
    #[error("validation error: {0}")]
    Validation(String),

    /// Process spawning or management error.
    #[error("process error: {0}")]
    Process(String),

    /// Network fault during topology or transport operations.
    #[error("Network fault error: {0}")]
    NetworkFault(String),

    /// Internal invariant violation.
    #[error("Invariant violation: {0}")]
    Invariant(String),

    /// Manifest I/O error.
    #[error("Manifest I/O error: {0}")]
    Manifest(String),

    /// Privilege error.
    #[error("Privilege error: {operation} requires {capability}")]
    Privilege {
        operation: String,
        capability: String,
    },

    /// I/O error.
    #[error("I/O error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },

    /// Chaos scheduling error.
    #[error("chaos error: {0}")]
    Chaos(String),

    /// YAML parse error.
    #[error("YAML parse error: {source}")]
    Yaml {
        #[from]
        source: serde_yaml::Error,
    },

    /// JSON serialization error.
    #[error("JSON error: {source}")]
    Json {
        #[from]
        source: serde_json::Error,
    },

    /// Base64 decode error.
    #[error("base64 decode error: {source}")]
    Base64 {
        #[from]
        source: base64::DecodeError,
    },

    /// Container backend error.
    #[error("container error: {0}")]
    Container(String),

    /// File not found.
    #[error("file not found: {path}")]
    FileNotFound { path: PathBuf },
}

/// Validation warning (non-fatal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationWarning {
    pub message: String,
}

impl std::fmt::Display for ValidationWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "warning: {}", self.message)
    }
}

pub type Result<T> = std::result::Result<T, EmulatorError>;

#[cfg(test)]
mod tests {
    use super::EmulatorError;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn new_variants_display_correctly_and_are_send_sync() {
        assert_send_sync::<EmulatorError>();

        let network = EmulatorError::NetworkFault("timeout".into());
        assert_eq!(network.to_string(), "Network fault error: timeout");

        let invariant = EmulatorError::Invariant("state mismatch".into());
        assert_eq!(invariant.to_string(), "Invariant violation: state mismatch");

        let manifest = EmulatorError::Manifest("missing manifest".into());
        assert_eq!(manifest.to_string(), "Manifest I/O error: missing manifest");

        let privilege = EmulatorError::Privilege {
            operation: "start node".into(),
            capability: "admin access".into(),
        };
        assert_eq!(
            privilege.to_string(),
            "Privilege error: start node requires admin access"
        );
    }
}
