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
    #[error("invalid btree page type value {0}")]
    BTreePageTypeError(u8),
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
        // FIXME: I don't think this works
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
        r.read_exact(&mut [0; 20])?;
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
#[derive(Debug)]
pub(crate) enum BTreePageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}
impl TryFrom<u8> for BTreePageType {
    type Error = HeaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            2 => Self::InteriorIndex,
            5 => Self::InteriorTable,
            10 => Self::LeafIndex,
            13 => Self::LeafTable,
            _ => return Err(HeaderError::BTreePageTypeError(value)),
        })
    }
}

#[derive(Debug)]
pub(crate) struct BTreeHeader {
    pub(crate) page_type: BTreePageType,
    /// zero if no freeblocks
    pub freeblock_start: u16,
    pub(crate) cell_count: u16,
    /// only u32 because 0 => 65536
    pub(crate) cell_start: u32,
    /// number of fragmented bytes within cell content area
    pub fragments: u8,
    /// only for InteriorIndex->rightmost pointer
    pub(crate) right_ptr: Option<u32>,
}
impl BTreeHeader {
    pub(crate) fn new(mut r: impl Read) -> anyhow::Result<Self> {
        let mut single_byte = [0];
        let mut two_bytes = [0; 2];
        let mut four_bytes = [0; 4];
        r.read_exact(&mut single_byte)?;
        let page_type = BTreePageType::try_from(single_byte[0])?;
        r.read_exact(&mut two_bytes)?;
        let freeblock_start = u16::from_be_bytes(two_bytes);
        r.read_exact(&mut two_bytes)?;
        let cell_count = u16::from_be_bytes(two_bytes);
        r.read_exact(&mut two_bytes)?;
        let cell_start = u16::from_be_bytes(two_bytes);
        let cell_start = if cell_start == 0 {
            0x10000
        } else {
            cell_start as u32
        };
        r.read_exact(&mut single_byte)?;
        let fragments = single_byte[0];
        let right_ptr = match page_type {
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => {
                r.read_exact(&mut four_bytes)?;
                Some(u32::from_be_bytes(four_bytes))
            }
            BTreePageType::LeafIndex | BTreePageType::LeafTable => None,
        };
        r.read_exact(&mut four_bytes)?;
        Ok(Self {
            page_type,
            freeblock_start,
            cell_count,
            cell_start,
            fragments,
            right_ptr,
        })
    }
}
