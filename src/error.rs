//! Errors & Result types for columnar CSV

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Arrow Error")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Object Store")]
    ObjectStore(#[from] object_store::Error)
}