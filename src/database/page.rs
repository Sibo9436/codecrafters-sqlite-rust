pub(crate) trait ReadCell {
    type Error;
    fn read_cell<T>(&self, index: usize) -> Result<T, Self::Error>;
}

pub(crate) struct BTreeTableLeaf {
    payload_size: usize,
    rowid: u64,
    /// the initial portion of the payload that does not spill to overflow pages
    non_spilling: usize,
    overflow_page: Option<usize>,
}
