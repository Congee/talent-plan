use flume;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Erro type for kvs
#[derive(Error, Debug)]
pub enum KvsError {
    /// Serde Error
    #[error("error de/serializing")]
    SerdeError(#[from] serde_json::Error),

    #[error("flume::RecvError")]
    RecvError(#[from] flume::RecvError),

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

impl Serialize for KvsError {
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for KvsError {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        Ok(KvsError::Whatever(anyhow::anyhow!("unreachable")))
    }
}
