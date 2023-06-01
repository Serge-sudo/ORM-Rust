#![forbid(unsafe_code)]

use rusqlite::types::ToSqlOutput;
use rusqlite::{Result, ToSql};
use std::{borrow::Cow, fmt};

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct ObjectId(i64);

impl From<i64> for ObjectId {
    fn from(val: i64) -> Self {
        Self(val)
    }
}

impl ObjectId {
    pub fn into_i64(&self) -> i64 {
        self.0
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "{:?}", self.0)
    }
}

impl ToSql for ObjectId {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.0))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    String,
    Bytes,
    Int64,
    Float64,
    Bool,
}

pub trait ObjectType {
    const TYPE: DataType;
}

macro_rules! impl_object_type {
    ($rust_type:ty, $typ:expr) => {
        impl ObjectType for $rust_type {
            const TYPE: DataType = $typ;
        }
    };
}

impl_object_type!(String, DataType::String);
impl_object_type!(Vec<u8>, DataType::Bytes);
impl_object_type!(i64, DataType::Int64);
impl_object_type!(f64, DataType::Float64);
impl_object_type!(bool, DataType::Bool);

////////////////////////////////////////////////////////////////////////////////

pub enum Value<'a> {
    String(Cow<'a, str>),
    Bytes(Cow<'a, [u8]>),
    Int64(i64),
    Float64(f64),
    Bool(bool),
}

macro_rules! impl_value_from {
    ($from_type:ty, $variant:ident) => {
        impl<'a> From<&'a $from_type> for Value<'static> {
            fn from(typ: &'a $from_type) -> Self {
                Value::$variant(*typ)
            }
        }

        impl<'a> From<Value<'a>> for $from_type {
            fn from(val: Value<'a>) -> Self {
                if let Value::$variant(x) = val {
                    return x;
                }
                panic!("Unexpected value variant");
            }
        }
    };
}

macro_rules! impl_cow_value_from {
    ($from_type:ty, $variant:ident) => {
        impl<'a> From<&'a $from_type> for Value<'a> {
            fn from(typ: &'a $from_type) -> Self {
                Value::$variant(Cow::Borrowed(typ))
            }
        }

        impl<'a> From<Value<'a>> for $from_type {
            fn from(val: Value<'a>) -> Self {
                if let Value::$variant(x) = val {
                    return x.into_owned();
                }
                panic!("Unexpected value variant");
            }
        }
    };
}

impl_cow_value_from!(String, String);
impl_cow_value_from!(Vec<u8>, Bytes);
impl_value_from!(i64, Int64);
impl_value_from!(f64, Float64);
impl_value_from!(bool, Bool);

impl<'a> ToSql for Value<'a> {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        match self {
            Value::String(s) => Ok(ToSqlOutput::from(s.as_ref())),
            Value::Bytes(b) => Ok(ToSqlOutput::from(b.as_ref())),
            Value::Int64(i) => Ok(ToSqlOutput::from(*i)),
            Value::Float64(f) => Ok(ToSqlOutput::from(*f)),
            Value::Bool(b) => Ok(ToSqlOutput::from(*b)),
        }
    }
}
