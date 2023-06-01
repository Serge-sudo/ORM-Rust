#![forbid(unsafe_code)]
use crate::{data::DataType, storage::Row};
use std::any::Any;

////////////////////////////////////////////////////////////////////////////////

pub trait Object: Any + Sized {
    const TABLE: &'static Schema;
    fn serialize(&self) -> Row;
    fn deserialize(row: Row) -> Self;
}

////////////////////////////////////////////////////////////////////////////////

pub struct Schema {
    pub table_name: &'static str,
    pub type_name: &'static str,
    pub columns: &'static [Column],
}

impl Schema {
    pub fn select_text(&self) -> String {
        let columns = if self.columns.is_empty() {
            "1".to_string()
        } else {
            self.columns
                .iter()
                .map(|c| c.column_name)
                .collect::<Vec<_>>()
                .join(", ")
        };

        format!("SELECT {} FROM {} WHERE id = ?", columns, self.table_name)
    }

    pub fn insert_text(&self) -> String {
        let fields: Vec<_> = self.columns.iter().map(|c| c.column_name).collect();
        let placeholders: Vec<_> = (0..self.columns.len()).map(|_| "?").collect();

        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.table_name,
            fields.join(", "),
            placeholders.join(", ")
        )
    }

    pub fn delete_text(&self) -> String {
        format!("DELETE FROM {} WHERE id = ?", self.table_name)
    }

    pub fn update_text(&self) -> String {
        let new_values: Vec<_> = self
            .columns
            .iter()
            .map(|c| format!("{} = ?", c.column_name))
            .collect();

        format!(
            "UPDATE {} SET {} WHERE id = ?",
            self.table_name,
            new_values.join(", ")
        )
    }
    pub fn create_text(&self) -> String {
        let mut query = format!(
            "CREATE TABLE {} (id INTEGER PRIMARY KEY AUTOINCREMENT",
            self.table_name
        );

        for column in self.columns {
            query.push_str(&format!(", {} {}", column.column_name, column.attr_name));
        }

        query.push(')');

        query
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct Column {
    pub column_name: &'static str,
    pub attr_name: &'static str,
    pub typ: DataType,
}
