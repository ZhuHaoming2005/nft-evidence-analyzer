use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DedupError {
    #[error("{stage}: {message}")]
    InvalidInput {
        stage: &'static str,
        message: String,
    },
    #[error("parquet schema error in {}: {message}", path.display())]
    ParquetSchema { path: PathBuf, message: String },
    #[error("parquet read error in {}: {message}", path.display())]
    ParquetRead { path: PathBuf, message: String },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("interrupted")]
    Interrupted,
    #[error("{0}")]
    Message(String),
}

impl DedupError {
    pub fn invalid(stage: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidInput {
            stage,
            message: message.into(),
        }
    }
}
