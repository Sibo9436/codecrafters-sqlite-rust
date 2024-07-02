use std::{fmt::Display, io::Read};

use thiserror::Error;

use crate::{database::varint::Varint, sql::syntax::DbValue};

#[derive(Debug, Error)]
pub(super) enum RecordError {
    #[error("reading record failed")]
    RecordReadError,
    #[error("internal error: {0}")]
    InternalError(String),
}

#[derive(Debug)]
pub(crate) enum Record {
    Null,
    Integer(i64),
    Float(f64),
    Blob(Vec<u8>),
    String(String),
    Zero,
}

impl Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Record::Null => write!(f, "NULL"),
            Record::Integer(v) => write!(f, "{v}"),
            Record::Float(v) => write!(f, "{v}"),
            Record::Blob(v) => write!(f, "{}", String::from_utf8_lossy(v)),
            Record::String(v) => v.fmt(f),
            Record::Zero => write!(f, "0"),
        }
    }
}

impl Record {
    fn read_type(t: RecordType, mut reader: impl Read) -> Result<Record, RecordError> {
        match t {
            RecordType::Null => Ok(Record::Null),
            RecordType::I8 => {
                let mut b = [0];
                reader
                    .read_exact(&mut b)
                    .map_err(|e| RecordError::InternalError(e.to_string()))?;
                Ok(Record::Integer(i8::from_be_bytes(b) as i64))
            }
            RecordType::I16 => {
                let mut b = [0, 0];
                reader
                    .read_exact(&mut b)
                    .map_err(|e| RecordError::InternalError(e.to_string()))?;
                Ok(Record::Integer(i16::from_be_bytes(b) as i64))
            }
            RecordType::I24 => {
                let mut b = [0; 4];
                reader
                    .read_exact(&mut b[1..])
                    .map_err(|e| RecordError::InternalError(e.to_string()))?;
                Ok(Record::Integer(i32::from_be_bytes(b) as i64))
            }
            RecordType::I32 => {
                let mut b = [0; 4];
                reader
                    .read_exact(&mut b)
                    .map_err(|e| RecordError::InternalError(e.to_string()))?;
                Ok(Record::Integer(i32::from_be_bytes(b) as i64))
            }
            RecordType::I48 => {
                let mut b = [0; 8];
                reader
                    .read_exact(&mut b[2..])
                    .map_err(|e| RecordError::InternalError(e.to_string()))?;
                Ok(Record::Integer(i64::from_be_bytes(b)))
            }
            RecordType::I64 => {
                let mut b = [0; 8];
                reader
                    .read_exact(&mut b)
                    .map_err(|e| RecordError::InternalError(e.to_string()))?;
                Ok(Record::Integer(i64::from_be_bytes(b)))
            }
            RecordType::F64 => {
                let mut b = [0; 8];
                reader
                    .read_exact(&mut b)
                    .map_err(|e| RecordError::InternalError(e.to_string()))?;
                Ok(Record::Float(f64::from_be_bytes(b)))
            }
            RecordType::Zero => Ok(Record::Integer(0)),
            RecordType::One => Ok(Record::Integer(1)),
            RecordType::Blob(n) => {
                let mut b = vec![0; n];
                reader
                    .read_exact(&mut b)
                    .map_err(|e| RecordError::InternalError(e.to_string()))?;
                Ok(Record::Blob(b))
            }
            RecordType::String(n) => {
                let mut b = vec![0; n];
                reader
                    .read_exact(&mut b)
                    .map_err(|e| RecordError::InternalError(e.to_string()))?;
                Ok(Record::String(
                    String::from_utf8(b).map_err(|e| RecordError::InternalError(e.to_string()))?,
                ))
            }
        }
    }
    pub(crate) fn read_row(mut reader: impl Read) -> Result<Vec<Self>, RecordError> {
        let (header_size, first_size) = Varint::read_sized(&mut reader)
            .map_err(|e| RecordError::InternalError(e.to_string()))?;
        let mut rest_of_header = vec![0; header_size.0 as usize - first_size];
        reader
            .read_exact(&mut rest_of_header)
            .map_err(|e| RecordError::InternalError(e.to_string()))?;
        let mut types = Vec::new();
        let mut rest_of_header = rest_of_header.as_slice();
        while let Ok(value) = Varint::read(&mut rest_of_header) {
            let t = match value.0 {
                0 => RecordType::Null,
                1 => RecordType::I8,
                2 => RecordType::I16,
                3 => RecordType::I24,
                4 => RecordType::I32,
                5 => RecordType::I48,
                6 => RecordType::I64,
                7 => RecordType::F64,
                8 => RecordType::Zero,
                9 => RecordType::One,
                10 | 11 => return Err(RecordError::RecordReadError),
                v if v >= 12 && v % 2 == 0 => RecordType::Blob(((v - 12) / 2) as usize),
                v if v >= 13 && v % 2 == 1 => RecordType::String(((v - 13) / 2) as usize),
                _ => return Err(RecordError::RecordReadError),
            };
            types.push(t);
        }
        types
            .into_iter()
            .map(|t| Record::read_type(t, &mut reader))
            .collect()
    }
}
enum RecordType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    Zero,
    One,
    Blob(usize),
    String(usize),
}

impl From<Record> for DbValue {
    fn from(value: Record) -> Self {
        match value {
            Record::Null => Self::Null,
            Record::Integer(i) => Self::Integer(i),
            Record::Float(f) => Self::Float(f),
            Record::Blob(b) => Self::Blob(b),
            Record::String(s) => Self::Text(s),
            Record::Zero => Self::Integer(0),
        }
    }
}
impl From<&Record> for DbValue {
    fn from(value: &Record) -> Self {
        value.clone().into()
    }
}
