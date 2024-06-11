use std::{collections::HashMap, fs::File, os::unix::fs::FileExt};

use crate::sql::{
    self, parse_sql,
    syntax::{
        ColType, ColumnConstraint, ColumnDefinition, CreateStatement, DbValue, Expr,
        SelectStatement, Statement, Visit,
    },
};

use self::{
    header::Header,
    record::Record,
    tree::{BTreeTableReader, PageSupplier},
};
use anyhow::{Context, Result};
use itertools::Itertools;

mod expression;
pub(crate) mod header;
mod page;
mod query;
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
                    .map(|(_rowid, row)| {
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
    pub(crate) fn run_query(&mut self, query: &str) -> Vec<Vec<Vec<DbValue>>> {
        let statements = parse_sql(query);
        let mut rows = Vec::new();
        for stmt in &statements {
            let qr = self.visit_statement(stmt);
            if let QueryStep::QueryResult(res) = qr {
                rows.push(res)
            }
        }
        rows
    }

    fn get_table_def(&mut self, from: &str) -> (i32, String) {
        if matches!(
            from,
            "sqlite_schema" | "sqlite_master" | "sqlite_temp_schema" | "sqlite_temp_master"
        ) {
            //sqlite_schema is always starting from page 1
            (1, schema::SCHEMA_DEF.to_string())
        } else {
            let query = format!("SELECT rootpage, sql FROM sqlite_schema WHERE name ='{from}'");
            let mut res = self.run_query(&query);
            println!("res of {query}:{res:?}");
            // WEARESOTHERE
            //unimplemented!("we are not there yet :)")
            // for now we panic
            let mut row = res.swap_remove(0).swap_remove(0);
            let rootpage = row.remove(0);
            let sql = row.remove(0);
            if let (DbValue::Integer(rootpage), DbValue::Text(sql)) = (rootpage, sql) {
                (rootpage as i32, sql)
            } else {
                unreachable!("what in the world happened to youuuu")
            }
        }
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

// NOTE: I'm not super sure abot this
enum QueryStep {
    FilterStep(Box<dyn Fn(&HashMap<String, DbValue>) -> DbValue>),
    QueryResult(Vec<Vec<DbValue>>),
    ExecuteResult,
}
impl Visit<QueryStep> for DbAccess {
    fn visit_expr<'a>(&'a mut self, e: &sql::syntax::Expr) -> QueryStep {
        //QueryStep::FilterStep(self.precompile_expr(e))
        todo!()
    }

    // TODO: restituire un Result e togliere tutti sti panics
    fn visit_statement(&mut self, e: &sql::syntax::Statement) -> QueryStep {
        match e {
            sql::syntax::Statement::Create(_) => {
                unimplemented!("execution of create statements not supported yet!")
            }
            sql::syntax::Statement::Select(SelectStatement {
                from,
                fields,
                filter,
            }) => {
                let (rootpage, table_def) = self.get_table_def(from);
                let table_def = &parse_sql(&table_def)[0];
                let col_pos: HashMap<&String, usize>;
                let columns;
                if let Statement::Create(CreateStatement::Table { name: _name, cols }) = table_def {
                    columns = cols;
                    col_pos = HashMap::from_iter(cols.iter().map(|c| (&c.name, c.position)));
                } else {
                    unreachable!("we should be in a create statement at this point of our lives")
                }
                let table_reader = BTreeTableReader {};
                let rows = match filter {
                    Some(filter) => {
                        let filter = expression::precompile_expr(filter);
                        table_reader
                            .scan_table(rootpage as usize, self, &|rowid, v| {
                                let row = Record::read_row(v).expect(":c");
                                print!("Read row: ");
                                for r in &row {
                                    print!("{rowid}:{r} ");
                                }
                                println!("---");
                                let row: Vec<DbValue> = row
                                    .into_iter()
                                    // TODO: sta roba è ripetuta già due volte e quindi va astratta
                                    // che qui fa schifio
                                    .map(|r| match r {
                                        Record::Null => DbValue::Null,
                                        Record::Integer(i) => DbValue::Integer(i),
                                        Record::Float(_) => unimplemented!("float :/"),
                                        Record::Blob(b) => DbValue::Blob(b),
                                        Record::String(s) => DbValue::Text(s),
                                        Record::Zero => DbValue::Integer(0),
                                    })
                                    .collect();
                                let mut map: HashMap<String, DbValue> = col_pos
                                    .clone()
                                    .into_iter()
                                    .map(|(k, v)| (k.clone(), row[v].clone()))
                                    .collect();
                                // FIXME: questo necessita di rework
                                for col in columns {
                                    // NOTE: We are looking for a INTEGER PRIMARY KEY columns,
                                    // which sqlite substitutes the rowid for automatically
                                    // FIXME: in più non mi piace per nulla
                                    if ColType::INTEGER == col.typ
                                        && col
                                            .constraint
                                            .iter()
                                            .any(|cont| matches!(cont, ColumnConstraint::Pk { .. }))
                                    {
                                        map.insert(col.name.clone(), DbValue::Integer(rowid));
                                    }
                                }

                                if let DbValue::Bool(b) = filter(&map) {
                                    b
                                } else {
                                    false
                                }
                            })
                            .expect("ahiahi mamacita")
                    }
                    None => table_reader
                        .scan_table(rootpage as usize, self, &|_, _| true)
                        .expect("ohnoes"),
                };
                println!("Q: {from:?} {filter:?}");
                for (i, (id, v)) in rows.iter().enumerate() {
                    println!("Row {i}[{id}]: {:?}", Record::read_row(v.as_slice()))
                }
                //SELECT *
                //code generation I guess lol kill me
                let empty;
                let mut fields = fields;
                if fields.is_empty() {
                    empty = col_pos
                        .iter()
                        .sorted_by_key(|(_, &k)| k)
                        .map(|(&k, _)| k)
                        .map(|s| Expr::Identifier { value: s.clone() })
                        .collect();
                    fields = &empty;
                }
                let mut f = Vec::new();
                for field in fields {
                    println!("Penso che stiamo morendo qui {field:?}");
                    let e = expression::precompile_expr(field);
                    f.push(e);
                }
                let mut res = Vec::new();
                for (rowid, row) in rows {
                    let row = Record::read_row(row.as_slice()).expect(":c");
                    let row: Vec<DbValue> = row
                        .into_iter()
                        .map(|r| match r {
                            Record::Null => DbValue::Null,
                            Record::Integer(i) => DbValue::Integer(i),
                            Record::Float(_) => unimplemented!("float :/"),
                            Record::Blob(b) => DbValue::Blob(b),
                            Record::String(s) => DbValue::Text(s),
                            Record::Zero => DbValue::Integer(0),
                        })
                        .collect();
                    let mut map: HashMap<String, DbValue> = col_pos
                        .clone()
                        .into_iter()
                        .map(|(k, v)| (k.clone(), row[v].clone()))
                        .collect();
                    // FIXME: questo necessita di rework
                    for col in columns {
                        // NOTE: We are looking for a INTEGER PRIMARY KEY columns,
                        // which sqlite substitutes the rowid for automatically
                        // FIXME: in più non mi piace per nulla
                        if ColType::INTEGER == col.typ
                            && col
                                .constraint
                                .iter()
                                .any(|cont| matches!(cont, ColumnConstraint::Pk { .. }))
                        {
                            map.insert(col.name.clone(), DbValue::Integer(rowid));
                        }
                    }
                    let row = f.iter().map(|expr| expr(&map)).collect();

                    res.push(row);
                }
                QueryStep::QueryResult(res)
            }
        }
    }

    fn visit_column_definition(&mut self, e: &sql::syntax::ColumnDefinition) -> QueryStep {
        todo!()
    }
}
