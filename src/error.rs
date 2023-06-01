#![forbid(unsafe_code)]

use crate::Error::{LockConflict, MissingColumn, Storage};
use crate::{data::DataType, object::Schema, ObjectId};
use rusqlite::Error::SqliteFailure;
use rusqlite::ErrorCode::DatabaseBusy;
use thiserror::Error;
////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    NotFound(Box<NotFoundError>),
    #[error(transparent)]
    UnexpectedType(Box<UnexpectedTypeError>),
    #[error(transparent)]
    MissingColumn(Box<MissingColumnError>),
    #[error("database is locked")]
    LockConflict,
    #[error("storage error: {0}")]
    Storage(#[source] Box<dyn std::error::Error>),
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ErrorWithCtx<T>
where
    T: std::error::Error,
{
    inner: T,
}

impl From<rusqlite::Error> for ErrorWithCtx<rusqlite::Error> {
    fn from(err: rusqlite::Error) -> Self {
        ErrorWithCtx { inner: err }
    }
}

////////////////////////////////////////////////////////////////////////////////
impl From<ErrorWithCtx<rusqlite::Error>> for Error {
    fn from(err: ErrorWithCtx<rusqlite::Error>) -> Self {
        match err.inner {
            SqliteFailure(err, _) if err.code != DatabaseBusy => Storage(Box::new(err)),
            SqliteFailure(_, _) => LockConflict,
            err => Storage(Box::new(err)),
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Self::from(ErrorWithCtx::from(err))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("object is not found: type '{type_name}', id {object_id}")]
pub struct NotFoundError {
    pub object_id: ObjectId,
    pub type_name: &'static str,
}

impl NotFoundError {
    pub fn new(object_id: ObjectId, type_name: &'static str) -> Self {
        Self {
            object_id,
            type_name,
        }
    }
}
////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error(
    "invalid type for {type_name}::{attr_name}: expected equivalent of {expected_type:?}, \
    got {got_type} (table: {table_name}, column: {column_name})"
)]
pub struct UnexpectedTypeError {
    pub type_name: &'static str,
    pub attr_name: &'static str,
    pub table_name: &'static str,
    pub column_name: &'static str,
    pub expected_type: DataType,
    pub got_type: String,
}

impl UnexpectedTypeError {
    pub fn new(
        type_name: &'static str,
        attr_name: &'static str,
        table_name: &'static str,
        column_name: &'static str,
        expected_type: DataType,
        got_type: String,
    ) -> Self {
        Self {
            type_name,
            attr_name,
            table_name,
            column_name,
            expected_type,
            got_type,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error(
    "missing a column for {type_name}::{attr_name} \
    (table: {table_name}, column: {column_name})"
)]
pub struct MissingColumnError {
    pub type_name: &'static str,
    pub attr_name: &'static str,
    pub table_name: &'static str,
    pub column_name: &'static str,
}

impl MissingColumnError {
    pub fn new(
        type_name: &'static str,
        attr_name: &'static str,
        table_name: &'static str,
        column_name: &'static str,
    ) -> Self {
        Self {
            type_name,
            attr_name,
            table_name,
            column_name,
        }
    }

    pub fn get_error_from_text(err_text: &str, schema: &Schema) -> Option<crate::Error> {
        let omit = match err_text {
            s if s.contains("no such column:") => {
                s.find("no such column:").unwrap() + "no such column:".len()
            }
            s if s.contains("has no column named") => {
                s.find("has no column named").unwrap() + "has no column named".len()
            }
            _ => return None,
        };

        let name = err_text[omit..].trim();
        schema
            .columns
            .iter()
            .find(|info| info.column_name == name)
            .map(|info| {
                MissingColumn(Box::new(MissingColumnError::new(
                    <&str>::clone(&schema.type_name),
                    <&str>::clone(&info.attr_name),
                    <&str>::clone(&schema.table_name),
                    <&str>::clone(&info.column_name),
                )))
            })
            .or_else(|| {
                if name == "id" {
                    Some(MissingColumn(Box::new(MissingColumnError::new(
                        <&str>::clone(&schema.type_name),
                        "id",
                        <&str>::clone(&schema.table_name),
                        "id",
                    ))))
                } else {
                    None
                }
            })
    }
}

////////////////////////////////////////////////////////////////////////////////
pub type Result<T> = std::result::Result<T, Error>;
