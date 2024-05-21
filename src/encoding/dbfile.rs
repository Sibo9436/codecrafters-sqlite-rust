use std::io::{self, Read};
use thiserror::Error;

pub(crate) enum RWMode {
    Journal,
    WAL,
    None,
}
impl From<&u8> for RWMode {
    fn from(value: &u8) -> Self {
        match value {
            1 => Self::Journal,
            2 => Self::WAL,
            _ => Self::None,
        }
    }
}
pub(crate) enum SchemaFormat {
    Format1,
    Format2,
    Format3,
    Format4,
}

impl From<u32> for SchemaFormat {
    fn from(value: u32) -> Self {
        println!("Format : {value}");
        match value {
            1 => Self::Format1,
            2 => Self::Format2,
            3 => Self::Format3,
            4 => Self::Format4,
            _ => panic!("Avrei dovuto mettere un try_from"),
        }
    }
}

pub(crate) enum TextEncoding {
    UTF8 = 1,
    UTF16le = 2,
    UTF16be = 3,
}
impl From<u32> for TextEncoding {
    fn from(value: u32) -> Self {
        match value {
            1 => Self::UTF8,
            2 => Self::UTF16le,
            3 => Self::UTF16be,
            _ => panic!("should have used tryfrom"),
        }
    }
}
pub(crate) struct Header {
    pub(crate) page_size: u32,
    pub(crate) write_version: RWMode,
    pub(crate) read_version: RWMode,
    pub(crate) reserved_size: RWMode,
    pub(crate) max_embedded_fraction: u8,
    pub(crate) min_embedded_fraction: u8,
    pub(crate) leaf_fraction: u8,
    pub(crate) file_change_counter: u32,
    pub(crate) size_in_pages: u32,
    pub(crate) first_freelist_page: u32,
    pub(crate) total_freelist: u32,
    pub(crate) schema_cookie: u32,
    pub(crate) schema_format_number: SchemaFormat,
    pub(crate) cache_size: u32,
    pub(crate) largest_root_btree_page: u32,
    pub(crate) text_encoding: TextEncoding,
    pub(crate) user_version: u32,
    pub(crate) incremental_vacuum: u32,
    pub(crate) version_valid_for: u32,
    pub(crate) sqlite_version: u32,
    application_id: u32,
}

#[derive(Error, Debug)]
pub(crate) enum HeaderError {
    #[error("file read error")]
    ReadError(#[from] io::Error),
    #[error("invalid file")]
    InvalidFile,
    #[error("fraction inconsistent")]
    InvalidFractionError,
    #[error("vacuum mode inconsistent")]
    VacuumModeError,
}

impl Header {
    pub(crate) fn new(mut r: impl Read) -> Result<Self, HeaderError> {
        let mut buf: [u8; 16] = [0; 16];
        r.read_exact(&mut buf)?;
        if &buf != b"SQLite format 3\0" {
            return Err(HeaderError::InvalidFile);
        }

        let mut b = [0; 2];
        r.read_exact(&mut b)?;
        let page_size = u16::from_be_bytes(b);
        let write_ver = &0;
        let read_ver = &0;
        r.read_exact(&mut [*write_ver, *read_ver])?;
        let _padding = &0;
        r.read_exact(&mut [*_padding])?;
        let mut c = [0; 3];
        r.read_exact(&mut c)?;
        if c != [64, 32, 32] {
            let header = Err(HeaderError::InvalidFractionError);
            return header;
        }
        //weird trick to make it unmut
        let c = c;
        let mut b = [0; 4];
        r.read_exact(&mut b)?;
        let change_counter = u32::from_be_bytes(b);
        r.read_exact(&mut b)?;
        let in_header_size = u32::from_be_bytes(b);
        r.read_exact(&mut b)?;
        let first_free_page = u32::from_be_bytes(b);
        r.read_exact(&mut b)?;
        let free_pages = u32::from_be_bytes(b);
        r.read_exact(&mut b)?;
        let schema_cookie = u32::from_be_bytes(b);
        r.read_exact(&mut b)?;
        let schema_format: SchemaFormat = u32::from_be_bytes(b).into();
        r.read_exact(&mut b)?;
        let suggested_cache = u32::from_be_bytes(b);
        r.read_exact(&mut b)?;
        let largest_root_btree = u32::from_be_bytes(b);
        r.read_exact(&mut b)?;
        let text_encoding: TextEncoding = u32::from_be_bytes(b).into();
        r.read_exact(&mut b)?;
        let user_version = u32::from_be_bytes(b);
        r.read_exact(&mut b)?;
        let vacuum_mode = u32::from_be_bytes(b);
        if largest_root_btree == 0 && vacuum_mode != 0 {
            return Err(HeaderError::VacuumModeError);
        }
        r.read_exact(&mut b)?;
        let application_id = u32::from_be_bytes(b);
        let _ = r.read_exact(&mut [0; 20])?;
        r.read_exact(&mut b)?;
        let version_valid_for = u32::from_be_bytes(b);
        r.read_exact(&mut b)?;
        let sqlite_version = u32::from_be_bytes(b);

        Ok(Header {
            //weird magic number stuff
            page_size: if sqlite_version >= 3007001 && page_size == 1 {
                65536
            } else {
                page_size as u32
            },
            write_version: write_ver.into(),
            read_version: read_ver.into(),
            reserved_size: RWMode::None,
            max_embedded_fraction: c[0],
            min_embedded_fraction: c[1],
            leaf_fraction: c[2],
            file_change_counter: change_counter,
            size_in_pages: if version_valid_for == change_counter {
                in_header_size
            } else {
                0
            },
            first_freelist_page: first_free_page,
            total_freelist: free_pages,
            schema_cookie,
            schema_format_number: schema_format,
            cache_size: suggested_cache,
            largest_root_btree_page: largest_root_btree,
            text_encoding,
            user_version,
            incremental_vacuum: vacuum_mode,
            application_id,
            version_valid_for,
            sqlite_version,
        })
    }
}
