use thiserror::Error;

/// Erro type for kvs
#[derive(Error, Debug)]
pub enum KvsError {
    /// Not found
    #[error("key not found")]
    KeyNotFound,

    /// IO Error
    #[error("IO error")]
    Io(#[from] std::io::Error),

    /// Utf8
    #[error("bytes to utf8")]
    Parsing(#[from] std::string::FromUtf8Error),

    /// Some application error
    #[error(transparent)]
    Whatever(#[from] anyhow::Error),
}

/// kvs Result type
pub type Result<T> = std::result::Result<T, KvsError>;
