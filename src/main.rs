use anyhow::{bail, Result};
use itertools::Itertools;
use std::fs::File;

use crate::database::DbAccess;

mod database;
mod sql;

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
    let file = File::open(&args[1])?;
    let mut dbaccess = DbAccess::new(file)?;
    match command.as_str() {
        ".dbinfo" => {
            let number_of_tables = dbaccess.number_of_tables()?;

            // You can use print statements as follows for debugging, they'll be visible when running tests.
            println!("Logs from your program will appear here!");

            // Uncomment this block to pass the first stage
            println!("database page size: {}", dbaccess.header.page_size);
            println!("number of tables: {}", number_of_tables);
            //println!("number of tables: {}", header.size);
        }
        ".tables" => {
            let names = dbaccess.table_names()?.join(" ");
            println!("{names}");
        }
        query => {
            let tables = dbaccess.run_query(query);
            for table in tables {
                println!("{}", table.columns.iter().map(|c| c.name()).join("\t|\t"));
                for row in table.rows {
                    println!("{row:?}");
                }
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
