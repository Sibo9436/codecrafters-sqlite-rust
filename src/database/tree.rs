use std::collections::VecDeque;
use std::io::Read;
use std::rc::Rc;
use std::usize;

use crate::database::varint::ReadVarint;
use crate::database::varint::Varint;
use crate::sql::syntax::DbValue;

use super::header::{BTreeHeader, BTreePageType};
use super::query::Row;
use super::query::RowCursor;
use super::record::Record;

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
impl Cell {
    fn read(typ: &BTreePageType, mut r: impl Read) -> Result<Cell, ()> {
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
}

pub(crate) trait PageSupplier {
    type Error;
    fn move_to_page(&mut self, page_idx: usize) -> Result<(), Self::Error>;
    fn read_page(&mut self, page_idx: usize) -> Result<&[u8], Self::Error>;
    fn page(&self) -> &[u8];
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
            //println!("Reading at {}", r.pos + header.cell_start as usize);
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

    fn read_cell(&self, typ: &BTreePageType, r: impl Read) -> Result<Cell, ()> {
        Cell::read(typ, r)
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

pub struct Cursor<'a, T> {
    supplier: &'a mut T,
    // dati successivi per capire cosa farmene
    // NOTE: this could be an iterator, is there a better way?
    /// holds the reference to the row we're currently on, if we overflow
    /// we know we have to change page
    row: Row,
    page_idx: i64,
    pages: VecDeque<i64>,
    header: BTreeHeader,
    pointers: VecDeque<usize>,
}

impl<'a, T: PageSupplier> Cursor<'a, T> {
    fn load_page(&mut self) -> Option<()> {
        let page_idx = self.pages.pop_front()?;
        let mut p = self.supplier.read_page(page_idx as usize).ok()?;
        let header = BTreeHeader::new(&mut p).ok()?;
        let mut pointers = vec![0; header.cell_count as usize];
        let mut b2 = [0, 0];
        for pointer in &mut pointers {
            p.read_exact(&mut b2).ok()?;
            *pointer = u16::from_be_bytes(b2) as usize;
        }
        let offset = if self.page_idx == 1 { 100 } else { 0 };
        match header.page_type {
            BTreePageType::InteriorIndex | BTreePageType::LeafIndex => {
                unimplemented!("indices: soon to be implemented!")
            }
            BTreePageType::InteriorTable => {
                // main idea is to do dfs -> I built a reverse stack cuz yeah
                for pointer in pointers.iter().rev() {
                    let cell =
                        Cell::read(&BTreePageType::InteriorTable, &p[pointer - offset..]).ok()?;
                    self.pages.push_front(cell.left_child as i64);
                }
                return self.load_page();
            }
            BTreePageType::LeafTable => {
                self.header = header;
                self.pointers = pointers.into();
                self.page_idx = page_idx;
                self.load_row()?;
            }
        }

        Some(())
    }
    fn load_row(&mut self) -> Option<()> {
        let ptr = self.pointers.pop_front()?;
        let offset = if self.page_idx == 1 { 100 } else { 0 };
        let page = &self.supplier.page()[ptr - offset..];
        let cell = Cell::read(&self.header.page_type, page).ok()?;
        let row = Record::read_row(cell.payload.as_slice()).ok()?;
        self.row = Row {
            id: DbValue::Integer(cell.rowid),
            row: row.iter().map(Into::into).collect(),
        };

        Some(())
    }
    pub fn new(supplier: &'a mut T, idx: i64) -> Self {
        // Un poco na merda ma amen
        let mut s = Self {
            supplier,
            row: Row {
                id: DbValue::Null,
                row: Vec::new(),
            },
            page_idx: idx,
            pages: VecDeque::new(),
            header: BTreeHeader {
                page_type: BTreePageType::InteriorTable,
                freeblock_start: 0,
                cell_count: 0,
                cell_start: 0,
                fragments: 0,
                right_ptr: Some(0),
            },
            pointers: VecDeque::new(),
        };
        s.next();
        s
    }
}

// NOTE: per adesso, per pigrizia, inizio ignorando il fatto che le cells all'interno di una pagina
// siano al contrario (le celle vengono riempite a partire dal fondo per non cazzare gli spazi
// vuoti)
//

impl<'a, T: PageSupplier> RowCursor for Cursor<'a, T> {
    fn column(&self, colpos: usize) -> Option<&DbValue> {
        self.row.row.get(colpos)
    }

    // WARNING: OCCHIO A QUESTA COSA:
    // il btree header è immediatamente seguito da un'array di cell pointers che io fino ad oggi
    // ho bellamente ignorato nessuno sa perché
    fn next(&mut self) -> Option<()> {
        if let None = self.load_row() {
            self.load_page()?
        }
        Some(())
    }

    fn rowid(&self) -> &DbValue {
        &self.row.id
    }
}
