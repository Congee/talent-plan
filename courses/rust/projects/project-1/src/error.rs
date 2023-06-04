use flume;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Erro type for kvs
#[derive(Error, Debug)]
pub enum KvsError {
    /// Serde Error
    #[error("error de/serializing")]
    SerdeError(#[from] serde_json::Error),

    #[error("flume error")]
    FlumeError(String),

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
    fn deserialize<D>(_: D) -> core::result::Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        Ok(KvsError::Whatever(anyhow::anyhow!("unreachable")))
    }
}

impl From<flume::SendError<crate::kv::StoreRep>> for KvsError  {
    fn from(value: flume::SendError<crate::kv::StoreRep>) -> Self {
        KvsError::FlumeError(value.to_string())
    }
}

impl From<flume::RecvError> for KvsError  {
    fn from(value: flume::RecvError) -> Self {
        KvsError::FlumeError(value.to_string())
    }
}

impl From<flume::SendError<crate::kv::StoreReq>> for KvsError  {
    fn from(value: flume::SendError<crate::kv::StoreReq>) -> Self {
        KvsError::FlumeError(value.to_string())
    }
}

