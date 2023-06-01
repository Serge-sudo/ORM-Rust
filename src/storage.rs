#![forbid(unsafe_code)]

use crate::error::NotFoundError;
use crate::Error::{NotFound, UnexpectedType};
use crate::{
    data::{DataType, Value},
    error::*,
    object::Schema,
    ObjectId,
};
use rusqlite::ToSql;
use std::borrow::Cow;

////////////////////////////////////////////////////////////////////////////////

pub type Row<'a> = Vec<Value<'a>>;
pub type RowSlice<'a> = [Value<'a>];

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait StorageTransaction {
    fn table_exists(&self, table: &str) -> Result<bool>;
    fn create_table(&self, schema: &Schema) -> Result<()>;

    fn insert_row(&self, schema: &Schema, row: &RowSlice) -> Result<ObjectId>;
    fn update_row(&self, id: ObjectId, schema: &Schema, row: &RowSlice) -> Result<()>;
    fn select_row(&self, id: ObjectId, schema: &Schema) -> Result<Row<'static>>;

    fn delete_row(&self, id: ObjectId, schema: &Schema) -> Result<()>;

    fn commit(&self) -> Result<()>;
    fn rollback(&self) -> Result<()>;
}

impl<'a> StorageTransaction for rusqlite::Transaction<'a> {
    fn table_exists(&self, table: &str) -> Result<bool> {
        let select_q = format!(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='{}'",
            table
        );
        let mut res = self.prepare_cached(&select_q)?;

        let exists = match res.query_row([], |_| Ok(())) {
            Ok(_) => true,
            Err(rusqlite::Error::QueryReturnedNoRows) => false,
            Err(e) => return Err(e.into()),
        };

        Ok(exists)
    }

    fn create_table(&self, schema: &Schema) -> Result<()> {
        if let Err(e) = self.execute(&schema.create_text(), []) {
            Err(e.into())
        } else {
            Ok(())
        }
    }

    fn insert_row(&self, schema: &Schema, row: &RowSlice) -> Result<ObjectId> {
        let (q, args) = if row.is_empty() {
            (
                format!("INSERT INTO {} DEFAULT VALUES", schema.table_name),
                Vec::new(),
            )
        } else {
            (
                schema.insert_text(),
                row.iter().map(|value| value as &dyn ToSql).collect(),
            )
        };
        match self.execute(&q, &args[..]) {
            Ok(result) if result == 1 => Ok(ObjectId::from(self.last_insert_rowid())),
            Err(e) => MissingColumnError::get_error_from_text(&e.to_string(), schema)
                .map_or_else(|| Err(e.into()), Err),
            _ => unreachable!(),
        }
    }

    fn update_row(&self, id: ObjectId, schema: &Schema, row: &RowSlice) -> Result<()> {
        if !schema.columns.is_empty() {
            let mut args = Vec::with_capacity(row.len() + 1);
            args.extend(row.iter().map(|value| value as &dyn ToSql));
            args.push(&id as &dyn ToSql);
            self.execute(&schema.update_text(), &args[..])?;
        }
        Ok(())
    }

    fn select_row(&self, id: ObjectId, schema: &Schema) -> Result<Row<'static>> {
        let select_q = self.prepare_cached(&schema.select_text());
        match select_q {
            Ok(mut result) => result.query_row([id.into_i64()], |row| {
                let mut line = vec![];
                let size = schema.columns.len();
                for i in 0..size {
                    let d_type = schema.columns[i].typ;
                    let value = match d_type {
                        DataType::Bytes => Value::Bytes(Cow::Owned(row.get(i)?)),
                        DataType::Int64 => Value::Int64(row.get(i)?),
                        DataType::String => Value::String(Cow::Owned(row.get(i)?)),
                        DataType::Float64 => Value::Float64(row.get(i)?),
                        DataType::Bool => Value::Bool(row.get(i)?),
                    };
                    line.push(value);
                }
                Ok(line)
            }),
            Err(err) => Err(err),
        }
        .map_err(|err| match err {
            rusqlite::Error::InvalidColumnType(i, _, type_n) => {
                UnexpectedType(Box::new(UnexpectedTypeError::new(
                    schema.type_name,
                    schema.columns[i].attr_name,
                    schema.table_name,
                    schema.columns[i].column_name,
                    schema.columns[i].typ,
                    type_n.to_string(),
                )))
            }
            rusqlite::Error::SqliteFailure(_, text) => {
                MissingColumnError::get_error_from_text(text.unwrap().as_str(), schema).unwrap()
            }
            _ => NotFound(Box::new(NotFoundError::new(id, schema.type_name))),
        })
    }

    fn delete_row(&self, id: ObjectId, schema: &Schema) -> Result<()> {
        let changes = self.execute(&schema.delete_text(), [id.into_i64()])?;

        if changes == 0 {
            return Err(NotFound(Box::new(NotFoundError::new(id, schema.type_name))));
        }

        Ok(())
    }

    fn commit(&self) -> Result<()> {
        if let Err(e) = self.execute("COMMIT", []) {
            Err(e.into())
        } else {
            Ok(())
        }
    }

    fn rollback(&self) -> Result<()> {
        if let Err(e) = self.execute("ROLLBACK", []) {
            Err(e.into())
        } else {
            Ok(())
        }
    }
}
