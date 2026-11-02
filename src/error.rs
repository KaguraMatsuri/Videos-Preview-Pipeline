use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("job or artifact not found")]
    NotFound,
    #[error("queue capacity reached")]
    QueueFull,
    #[error("a worker process already owns this database")]
    WorkerLocked,
    #[error("operation cancelled")]
    Cancelled,
    #[error("operation timed out")]
    Timeout,
    #[error("{0}")]
    Operation(String),
    #[error("filesystem operation failed")]
    Io(#[from] std::io::Error),
    #[error("database operation failed")]
    Database(#[from] rusqlite::Error),
    #[error("invalid JSON data")]
    Json(#[from] serde_json::Error),
}

impl Error {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Config(_) => "invalid_config",
            Self::Invalid(_) => "invalid_request",
            Self::NotFound => "not_found",
            Self::QueueFull => "queue_full",
            Self::WorkerLocked => "worker_locked",
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::Operation(_) => "operation_failed",
            Self::Io(_) => "io_error",
            Self::Database(_) => "database_error",
            Self::Json(_) => "invalid_json",
        }
    }
}
