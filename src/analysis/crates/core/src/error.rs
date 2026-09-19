use thiserror::Error;

/// Top-level error for the analysis pipeline.
#[derive(Debug, Error)]
pub enum AnalysisError {
    #[error("invalid input: {0}")]
    Invalid(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("parquet error: {0}")]
    Parquet(String),

    #[error("http error: {0}")]
    Http(String),

    #[error("cancelled")]
    Cancelled,
}

impl AnalysisError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }

    pub fn parquet(message: impl Into<String>) -> Self {
        Self::Parquet(message.into())
    }

    pub fn http(message: impl Into<String>) -> Self {
        Self::Http(message.into())
    }
}

impl From<String> for AnalysisError {
    fn from(value: String) -> Self {
        Self::Invalid(value)
    }
}

impl From<&str> for AnalysisError {
    fn from(value: &str) -> Self {
        Self::Invalid(value.to_owned())
    }
}
