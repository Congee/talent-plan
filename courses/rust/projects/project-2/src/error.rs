use thiserror::Error;
use std::io;

/// Error type for kvs.
#[derive(Error, Debug)]
pub enum KvsError {
    /// IO error.
    #[error("IO error")]
    Io(#[from] io::Error),
    /// Serialization or deserialization error.
    #[error("Serde error")]
    Serde(#[from] serde_json::Error),
    /// Removing non-existent key error.
    #[error("Key not found")]
    KeyNotFound,
    /// Unexpected command type error.
    /// It indicated a corrupted log or a program bug.
    #[error("Unexpected command type")]
    UnexpectedCommandType,
}

// impl From<io::Error> for KvsError {
//     fn from(err: io::Error) -> KvsError {
//         KvsError::Io(err)
//     }
// }
//
// impl From<serde_json::Error> for KvsError {
//     fn from(err: serde_json::Error) -> KvsError {
//         KvsError::Serde(err)
//     }
// }

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;
