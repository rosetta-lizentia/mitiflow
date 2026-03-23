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
