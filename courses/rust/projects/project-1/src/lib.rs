#![allow(missing_docs)]
#![feature(type_alias_impl_trait)]
#![feature(async_iterator)]
#![feature(box_into_inner)]
#![feature(sync_unsafe_cell)]
//! A simple key/value store.

pub use error::{Result, KvsError};
pub use kv::KvStore;

pub mod error;
pub mod kv;
pub mod xchg;
pub mod fs;

mod util;
