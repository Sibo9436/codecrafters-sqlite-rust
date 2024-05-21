use anyhow::{bail, Result};
use std::fs::File;
use std::io::BufReader;

use crate::encoding::dbfile::Header;

mod encoding;

// I have made the very unwise decision of not using any kind of parsing library
// parsing is fun and I want to do it on my own
fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let file = File::open(&args[1])?;
            let mut _header = [0; 100];

            let reader = BufReader::new(file);
            let header = Header::new(reader)?;

            // You can use print statements as follows for debugging, they'll be visible when running tests.
            println!("Logs from your program will appear here!");

            // Uncomment this block to pass the first stage
            println!("database page size: {}", header.page_size);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
