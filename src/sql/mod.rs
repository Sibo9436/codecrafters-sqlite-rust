use self::{parser::Parser, syntax::Statement};

mod lexer;
mod parser;
pub(crate) mod syntax;

pub(crate) fn parse_sql(sql: &str) -> Vec<Statement> {
    let toks = lexer::scan(&sql);
    let mut p = Parser::new(&toks);
    p.scan()
}
