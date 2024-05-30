use std::{fmt::Display, io::Read};

use thiserror::Error;

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct Varint(pub i64);

pub(super) trait ReadVarint {
    fn read_varint(&mut self) -> Result<Varint, VarintError>;
}

impl<T: Read> ReadVarint for T {
    fn read_varint(&mut self) -> Result<Varint, VarintError> {
        Varint::read(self)
    }
}

impl Display for Varint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Error, Debug)]
pub(crate) struct VarintError;
impl Display for VarintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "error while reading varint")
    }
}

impl Varint {
    pub(crate) fn read_sized(mut r: impl Read) -> Result<(Varint, usize), VarintError> {
        let mut res: u64 = 0;
        let mut buf = [0];
        let mut counter = 1;
        while let Ok(()) = r.read_exact(&mut buf) {
            let ptr = &buf[0];
            res <<= 7;
            res |= (*ptr & 0x7F) as u64;
            if *ptr & 0x80 == 0 || counter == 9 {
                return Ok((Varint(res as i64), counter));
            }
            counter += 1;
        }
        Err(VarintError)
    }
    pub(crate) fn read(r: impl Read) -> Result<Varint, VarintError> {
        Self::read_sized(r).map(|v| v.0)
    }
}

#[cfg(test)]
mod test {
    use crate::database::varint::Varint;

    #[test]
    fn test_decoding() -> Result<(), anyhow::Error> {
        let ar = [134_u8, 195_u8, 23_u8];
        assert_eq!(Varint(106903), Varint::read(ar.as_slice())?);
        let ar = [0];
        assert_eq!(Varint(0), Varint::read(ar.as_slice())?);
        let ar = [0x81, 0x00];
        assert_eq!(Varint(128), Varint::read(ar.as_slice())?);
        let ar = [0xc0, 0x00];
        assert_eq!(Varint(8192), Varint::read(ar.as_slice())?);
        let ar = [0xff, 0x7f];
        assert_eq!(Varint(16383), Varint::read(ar.as_slice())?);
        let ar = [0xff, 0xff, 0xff, 0x7f];
        assert_eq!(Varint(268435455), Varint::read(ar.as_slice())?);
        Ok(())
    }
}
