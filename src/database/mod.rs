use std::{fs::File, os::unix::fs::FileExt};

use self::{
    header::Header,
    record::Record,
    tree::{BTreeTableReader, PageSupplier},
};
use anyhow::{Context, Result};

pub(crate) mod header;
mod page;
mod record;
mod schema;
mod tree;
mod varint;

/// Struct (deal with it functional bros jk ily) that handles interaction with the on disk database
/// file
pub(crate) struct DbAccess {
    pub header: Header,
    // NOTE: Non so se sia meglio magari usare solo un Read, però tanto io su un file devo scrivere
    dbfile: File,
    page: Vec<u8>,
    /// Holds the start offset of a specific page, only
    /// useful for the first page start
    start_offset: usize,
}

impl DbAccess {
    /// Create a new db
    pub(crate) fn new(dbfile: File) -> Result<Self> {
        let mut buf = [0; 100];
        dbfile.read_exact_at(&mut buf, 0)?;
        let header = Header::new(buf.as_slice())?;
        let page_size = header.page_size as usize;
        Ok(Self {
            header,
            dbfile,
            page: vec![0; page_size],
            start_offset: 0,
        })
    }

    /// Reads required page into memory for later analysis
    fn seek_page(&mut self, page_number: usize) -> Result<()> {
        let page_idx = (page_number as u64 - 1) * self.header.page_size as u64;
        self.dbfile.read_exact_at(&mut self.page, page_idx)?;
        self.start_offset = if page_number == 1 { 100 } else { 0 };
        Ok(())
    }

    /// reads the header of a btree page
    fn btree_header(&mut self) -> Result<header::BTreeHeader> {
        header::BTreeHeader::new(&self.page[self.start_offset..])
            .context("could not read btree page header")
    }

    fn load_schema(&mut self) -> Result<()> {
        self.seek_page(1)?;
        let schema_header = self.btree_header()?;

        todo!()
    }

    fn seek_table_page(&mut self, table_name: &str) -> Result<usize> {
        todo!()
    }

    /// Reads schema table
    /// NOTE: non deve essere pubblica e dovrà restituire uno schema :)
    pub(crate) fn read_schema(&mut self) -> Result<Vec<Vec<Record>>> {
        let table_reader = BTreeTableReader {};
        table_reader
            .find_all_in_table(1, self)
            .map_err(|_| anyhow::format_err!("shit"))
            .and_then(|mut v| {
                v.iter_mut()
                    .map(|row| {
                        Record::read_row(row.as_slice()).map_err(|e| anyhow::anyhow!("{e:?}"))
                    })
                    .collect()
            })
    }

    pub(crate) fn number_of_tables(&mut self) -> Result<usize> {
        self.read_schema().map(|v| v.len())
    }

    pub(crate) fn table_names(&mut self) -> Result<Vec<String>> {
        self.read_schema()?
            .into_iter()
            .map(|v| match v.get(1) {
                Some(Record::String(s)) => Ok(s.clone()),
                _ => Err(anyhow::anyhow!("invalid schema")),
            })
            .collect()
    }
}
impl PageSupplier for DbAccess {
    type Error = anyhow::Error;
    fn move_to_page(&mut self, page_idx: usize) -> anyhow::Result<()> {
        self.seek_page(page_idx)
    }

    fn read_page(&mut self, page_idx: usize) -> anyhow::Result<&[u8]> {
        self.move_to_page(page_idx)?;
        Ok(&self.page[self.start_offset..])
    }
}
