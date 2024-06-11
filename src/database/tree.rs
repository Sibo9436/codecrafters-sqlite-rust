use std::io::Read;
use std::usize;

use crate::database::varint::ReadVarint;
use crate::database::varint::Varint;

use super::header::{BTreeHeader, BTreePageType};

struct IndexedReader<R: Read> {
    r: R,
    pos: usize,
}

impl<R: Read> IndexedReader<R> {
    fn new(r: R) -> Self {
        Self { r, pos: 0 }
    }
}

impl<R: Read> Read for IndexedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let res = self.r.read(buf)?;
        self.pos += res;
        Ok(res)
    }
}

pub(crate) struct Cell {
    rowid: i64,
    left_child: i32,
    //no need for payload size
    payload: Vec<u8>,
    first_overflow: Option<usize>,
}

pub(crate) trait PageSupplier {
    type Error;
    fn move_to_page(&mut self, page_idx: usize) -> Result<(), Self::Error>;
    fn read_page(&mut self, page_idx: usize) -> Result<&[u8], Self::Error>;
}

pub(crate) trait PageConsumer {
    // TODO:::
}

pub(crate) struct BTreeTableReader {}

impl BTreeTableReader {
    #[allow(unreachable_code)]
    pub(crate) fn scan_table<F>(
        &self,
        root_idx: usize,
        supplier: &mut impl PageSupplier,
        predicate: &F,
    ) -> Result<Vec<(i64, Vec<u8>)>, ()>
    where
        F: Fn(i64, &[u8]) -> bool,
    {
        // TODO: mettere a posto gestione degli errori qui
        supplier
            .move_to_page(root_idx)
            .map_err(|_| eprintln!("page supplier error "))?;
        let r = supplier
            .read_page(root_idx)
            .map_err(|_| eprintln!("page supplier error in read_page"))?;
        //println!("{r:?}");
        let header = BTreeHeader::new(r).map_err(|e| eprintln!("{e}"))?;
        println!("BTREEHEADER: {header:?}");
        let cell_start = header.cell_start - if root_idx == 1 { 100 } else { 0 };
        let r = &r[cell_start as usize..r.len()];
        let mut r = IndexedReader::new(r);
        // consume page cells based on page type
        // TODO: the code to read cells should be moved outside
        // FIXME: We should also add support for freeblock reading
        // My idea is to keep this match but read cells in a separate function that returns
        // the result vec and the number of bytes read, this way we can check whether we are about
        // to read the next freeblock and consume that instead
        let mut freeblock = header.freeblock_start as usize;
        let mut cells = Vec::new();
        loop {
            println!("Reading at {}", r.pos + header.cell_start as usize);
            if freeblock != 0 && r.pos + header.cell_start as usize == freeblock {
                println!("Encountered freeblock at {freeblock}");
                let mut buf = [0, 0];
                r.read_exact(&mut buf).map_err(|e| eprintln!("{e}"))?;
                freeblock = u16::from_be_bytes(buf) as usize;
                r.read_exact(&mut buf).map_err(|e| eprintln!("{e}"))?;
                // freeblock size includes 4 byte-header
                let size = u16::from_be_bytes(buf) as usize - 4;
                //there has to be a better way
                let mut v = vec![0; size];
                r.read_exact(&mut v).map_err(|e| eprintln!("{e}"))?;
            } else {
                if let Ok(cell) = self.read_cell(&header.page_type, &mut r) {
                    cells.push(cell);
                } else {
                    break;
                }
            }
        }
        match &header.page_type {
            BTreePageType::InteriorIndex | BTreePageType::LeafIndex => {
                unimplemented!("we do not support query by index yet!")
            }
            BTreePageType::InteriorTable => {
                let mut res = Vec::new();
                for cell in cells.iter().rev() {
                    res.append(&mut self.scan_table(
                        cell.left_child as usize,
                        supplier,
                        predicate,
                    )?)
                }
                Ok(res)
            }
            BTreePageType::LeafTable => {
                // TODO: we have to do the rowid magic so we'll later change this return type
                Ok(cells
                    .into_iter()
                    .rev()
                    .filter(|c| predicate(c.rowid, &c.payload))
                    .map(|c| (c.rowid, c.payload))
                    .collect())
            }
        }
    }

    fn read_cell(&self, typ: &BTreePageType, mut r: impl Read) -> Result<Cell, ()> {
        //the single match reduces branches but yields more code repetition
        match typ {
            BTreePageType::InteriorIndex => {
                let mut page_number_buf = [0; 4];
                r.read_exact(&mut page_number_buf)
                    .map_err(|e| eprintln!("{e}"))?;
                let left_child = i32::from_be_bytes(page_number_buf);
                let payload_size = Varint::read(&mut r).map_err(|e| eprintln!("{e}"))?;
                let mut payload = vec![0; payload_size.0.try_into().map_err(|e| eprintln!("{e}"))?];
                r.read_exact(&mut payload).map_err(|e| eprintln!("{e}"))?;
                //TODO: read start of overflow page
                Ok(Cell {
                    rowid: 0,
                    left_child,
                    payload,
                    first_overflow: None,
                })
            }
            BTreePageType::InteriorTable => {
                let mut page_number_buf = [0; 4];
                r.read_exact(&mut page_number_buf)
                    .map_err(|e| eprintln!("{e}"))?;
                let left_child = i32::from_be_bytes(page_number_buf);
                let rowid = r.read_varint().map_err(|e| eprintln!("{e}"))?.0;
                Ok(Cell {
                    rowid,
                    left_child,
                    payload: Vec::new(),
                    first_overflow: None,
                })
            }

            BTreePageType::LeafIndex => {
                let payload_size = Varint::read(&mut r).map_err(|e| eprintln!("{e}"))?;
                let mut payload = vec![0; payload_size.0.try_into().map_err(|e| eprintln!("{e}"))?];
                r.read_exact(&mut payload).map_err(|e| eprintln!("{e}"))?;
                //TODO: read start of overflow page
                Ok(Cell {
                    rowid: 0,
                    left_child: 0,
                    payload,
                    first_overflow: None,
                })
            }
            BTreePageType::LeafTable => {
                let payload_size = Varint::read(&mut r).map_err(|e| eprintln!("{e}"))?;
                let rowid = r.read_varint().map_err(|e| eprintln!("{e}"))?.0;
                let mut payload = vec![0; payload_size.0.try_into().map_err(|e| eprintln!("{e}"))?];
                r.read_exact(&mut payload).map_err(|e| eprintln!("{e}"))?;
                //TODO: read start of overflow page
                Ok(Cell {
                    rowid,
                    left_child: 0,
                    payload,
                    first_overflow: None,
                })
            }
        }
    }

    #[allow(unreachable_code)]
    pub(crate) fn find_all_in_table(
        &self,
        root_idx: usize,
        supplier: &mut impl PageSupplier,
    ) -> Result<Vec<(i64, Vec<u8>)>, ()> {
        self.scan_table(root_idx, supplier, &|_, _| true)
    }
}
