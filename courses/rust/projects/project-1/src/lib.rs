#![allow(missing_docs)]
#![feature(type_alias_impl_trait)]
#![feature(async_iterator)]
#![feature(box_into_inner)]
#![feature(sync_unsafe_cell)]
//! A simple key/value store.

pub use client::run_client;
pub use error::{KvsError, Result};
pub use server::run_server;
pub use kv::KvStore;

pub mod client;
pub mod error;
pub mod executor;
pub mod fs;
pub mod kv;
pub mod readv_all;
pub mod server;
pub mod xchg;

mod util;
