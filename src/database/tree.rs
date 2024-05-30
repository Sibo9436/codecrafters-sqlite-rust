use std::{error::Error, io::Read};

use crate::database::varint::Varint;

use super::header::{BTreeHeader, BTreePageType};

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
        predicate: F,
    ) -> Result<Vec<Vec<u8>>, ()>
    where
        F: Fn(&[u8]) -> bool,
    {
        // TODO: mettere a posto gestione degli errori qui
        supplier
            .move_to_page(root_idx)
            .map_err(|e| eprintln!("page supplier error "))?;
        let r = supplier
            .read_page(root_idx)
            .map_err(|e| eprintln!("page supplier error in read_page"))?;
        //println!("{r:?}");
        let header = BTreeHeader::new(r).map_err(|e| eprintln!("{e}"))?;
        let cell_start = header.cell_start - if root_idx == 1 { 100 } else { 0 };
        let mut r = &r[cell_start as usize..r.len()];
        // consume page cells based on page type
        // TODO: the code to read cells should be moved outside
        match header.page_type {
            // NOTE: Mi chiedo se abbia senso che "find all in table" parsi anche gli indici
            // La mia idea ora è di fare un po' un refactor del codice per consumare le celle
            // in modo da non dovere fare più funzioni identiche per supportare i diversi tipi di
            // query
            // La logica "giusta" sarebbe usare la stessa funzione di ora ma con un predicato e
            // aggiungere a res solo se il predicato è true, ma visto che la find usa anche gli
            // indici...
            // FIXME:??
            BTreePageType::InteriorIndex => {
                let mut pages = Vec::new();
                let mut page_number_buf = [0; 4];
                // NOTE: leggo tutte le celle prima per evitare di cambiare pagina mentre leggo
                //(grazie rust per avermelo fatto notare)
                while let Ok(()) = r.read_exact(&mut page_number_buf) {
                    let vint_key = Varint::read(r).map_err(|e| eprintln!("{e}"))?;
                    let page_num = i32::from_be_bytes(page_number_buf);
                    //println!("Interior tree cell has key {vint_key} at page {page_num}");
                    pages.push((vint_key, page_num));
                    let payload_size =
                        Varint::read(&mut r).map_err(|e| eprintln!("{e}"))?.0 as usize;
                    let mut payload = vec![0; payload_size];
                    let _ = r.read_exact(&mut payload).map_err(|e| eprintln!("{e}"))?;
                    //indices.append(payload);
                }
                let mut res = Vec::new();
                for (_, page) in pages {
                    res.append(&mut self.find_all_in_table(page as usize, supplier)?);
                }
                unimplemented!("We still do not support scanning by indices");
                Ok(res)
            }
            BTreePageType::LeafIndex => {
                let mut res = Vec::new();
                while let Ok(payload_size) = Varint::read(&mut r) {
                    let mut payload = vec![0; payload_size.0 as usize];
                    // NOTE: what if instead of passing the reader into functions
                    // I just implemented traits for all readers ???
                    let _ = r.read_exact(&mut payload).map_err(|e| eprintln!("{e}"))?;
                    // NOTE: omitting overflow page (4 bytes)

                    res.push(payload);
                }
                unimplemented!("We still do not support scanning by indices");
                Ok(res)
            }
            BTreePageType::InteriorTable => {
                let mut pages = Vec::new();
                let mut page_number_buf = [0; 4];
                // NOTE: leggo tutte le celle prima per evitare di cambiare pagina mentre leggo
                //(grazie rust per avermelo fatto notare)
                while let Ok(()) = r.read_exact(&mut page_number_buf) {
                    let vint_key = Varint::read(r).map_err(|e| eprintln!("{e}"))?;
                    let page_num = i32::from_be_bytes(page_number_buf);
                    //println!("Interior tree cell has key {vint_key} at page {page_num}");
                    pages.push((vint_key, page_num));
                }
                let mut res = Vec::new();
                for (_, page) in pages {
                    res.append(&mut self.find_all_in_table(page as usize, supplier)?);
                }
                Ok(res)
            }
            BTreePageType::LeafTable => {
                let mut res = Vec::new();
                // NOTE: se non metti &mut legge su una copia or something e non avanza il
                // puntatore
                while let Ok(payload_size) = Varint::read(&mut r) {
                    let _rowid = Varint::read(&mut r).map_err(|e| eprintln!("{e}"))?;
                    // NOTE: for now I'm ignoring overflowing pages
                    let mut payload = vec![0; payload_size.0 as usize];
                    r.read_exact(payload.as_mut_slice())
                        .map_err(|e| eprintln!("{e}"))?;
                    // NOTE: omitting overflow page (4 bytes)
                    //println!("Read row with id {rowid} size: {payload_size}\n{payload:?}");
                    if predicate(&payload) {
                        res.push(payload);
                    }
                }
                Ok(res)
            }
        }
    }
    #[allow(unreachable_code)]
    pub(crate) fn find_all_in_table(
        &self,
        root_idx: usize,
        supplier: &mut impl PageSupplier,
    ) -> Result<Vec<Vec<u8>>, ()> {
        self.scan_table(root_idx, supplier, |_| true)
    }
}
