//! Definition of our syntax
//! sql-stmt-list : sql-stmt  ( ';',sql-stmt)*;
//! sql-stms : create-table-stmt | select-stmt;
//! create-table-stmt : 'CREATE TABLE'  IDENT  '('  column-def  (',' column-def)+ ')';
//! column-def: IDENT type-name column-constraint?;
//! type-name : name ( '(' signed-number ')')?
//! signed-number: ('+'|'-')? NUM
//! column-constraint: 'PRIMARY' 'KEY'; // SI NON NE SUPPORTO MOLTI
//!

use thiserror::Error;

use super::syntax::{self, ColType, ColumnConstraint, ConflictClause, Expr, FunctionArg, Operator};

use super::lexer::{Token, TokenType};

// FIXME: This is not the best way I could think of but it's the one that would not take me a
// century
#[derive(Debug)]
pub(crate) struct Parser<'a> {
    tokens: &'a [Token<'a>],
    idx: usize,
}

#[derive(Debug, Error)]
pub(crate) enum ParseError {
    #[error("No further input found")]
    NoInput,

    #[error("invalid keyword [{0}] in statement")]
    InvalidKeyword(String),
    #[error("expected token {0}")]
    ExpectedToken(TokenType),
    #[error("{0}")]
    CustomError(&'static str),
}

impl<'a> Parser<'a> {
    pub(crate) fn new(tokens: &'a [Token<'a>]) -> Self {
        Self { tokens, idx: 0 }
    }
    // NOTE: non uso Peek o Iterator perché voglio poter tornare indietro, giusto???
    fn step(&mut self) -> Option<&Token<'a>> {
        let t = self.tokens.get(self.idx);
        self.idx += 1;
        t
    }
    fn peek(&self) -> Option<&Token<'a>> {
        self.tokens.get(self.idx)
    }

    pub(crate) fn scan(&mut self) -> Vec<syntax::Statement> {
        let mut statements = Vec::new();
        loop {
            let stmt = match self.statement() {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("{e}");
                    break;
                }
            };
            //println!("{stmt:?}");
            match self.step() {
                Some(tok) if tok.typ == TokenType::SEMICOLON => {
                    statements.push(stmt);
                }
                Some(tok) if tok.typ == TokenType::EOF => {
                    statements.push(stmt);
                    break;
                }
                Some(v) => panic!("invalid statement terminating character {}", v.lexeme),
                None => panic!("invalid statement termination"),
            }
        }
        statements
    }
    pub(crate) fn statement(&mut self) -> Result<syntax::Statement, ParseError> {
        let first = self.step().ok_or(ParseError::NoInput)?;
        match first.typ {
            TokenType::CREATE => self.create(),
            TokenType::SELECT => Ok(syntax::Statement::Select(self.select()?)),
            _ => Err(ParseError::InvalidKeyword(first.lexeme.to_owned())),
        }
    }

    fn create(&mut self) -> Result<syntax::Statement, ParseError> {
        let first = self.step().ok_or(ParseError::NoInput)?;
        match first.typ {
            TokenType::TABLE => Ok(syntax::Statement::Create(self.create_table()?)),
            _ => Err(ParseError::InvalidKeyword(first.lexeme.to_owned())),
        }
    }

    fn select(&mut self) -> Result<syntax::SelectStatement, ParseError> {
        let mut col_names = Vec::new();
        if self.peek().ok_or(ParseError::NoInput)?.typ == TokenType::ASTERISK {
            // NOTE: no columns means all colums because I said so :)
            self.step().expect("this should be impossible");
            self.expect(TokenType::FROM)?;
        } else {
            loop {
                let result_col = self.expression()?;
                let nxt = self.step().ok_or(ParseError::NoInput)?;
                match nxt.typ {
                    TokenType::FROM => {
                        col_names.push(result_col);
                        break;
                    }
                    TokenType::COMMA => {
                        col_names.push(result_col);
                    }
                    _ => return Err(ParseError::InvalidKeyword(nxt.lexeme.to_owned())),
                }
            }
        }
        let from = self.step().ok_or(ParseError::NoInput)?.lexeme.to_owned();
        let mut filter = None;
        if self.peek().ok_or(ParseError::NoInput)?.typ == TokenType::WHERE {
            self.expect(TokenType::WHERE)?;
            //println!("Yoo");
            filter = Some(self.expression()?);
        }
        Ok(syntax::SelectStatement {
            from,
            fields: col_names,
            filter,
        })
    }

    fn expect(&mut self, typ: TokenType) -> Result<&Token<'a>, ParseError> {
        match self.step() {
            Some(tok) if tok.typ == typ => Ok(tok),
            _ => Err(ParseError::ExpectedToken(typ)),
        }
    }
    /// Checks whether the condition is satisfied by the next token and in that case advances
    fn matches<F>(&mut self, f: F, error: &'static str) -> Result<&Token<'a>, ParseError>
    where
        F: Fn(&Token<'a>) -> bool,
    {
        match self.peek() {
            Some(tok) if f(tok) => Ok(self.step().expect("this should not be happening at all")),
            _ => Err(ParseError::CustomError(error)),
        }
    }

    fn create_table(&mut self) -> Result<syntax::CreateStatement, ParseError> {
        let name = self.step().ok_or(ParseError::NoInput).and_then(|tok| {
            if tok.typ == TokenType::IDENTIFIER {
                Ok(tok.lexeme)
            } else {
                Err(ParseError::InvalidKeyword(tok.lexeme.to_string()))
            }
        })?;
        self.expect(TokenType::OPENP)?;
        let mut cols = Vec::new();
        let mut i = 0;
        loop {
            let col_def = self.col_def(i)?;
            i += 1;
            let next = self.step().ok_or(ParseError::NoInput)?;
            match next.typ {
                TokenType::CLOSEP => {
                    cols.push(col_def);
                    break;
                }
                TokenType::COMMA => cols.push(col_def),
                _ => return Err(ParseError::InvalidKeyword(next.lexeme.to_string())),
            }
        }
        Ok(syntax::CreateStatement::Table {
            name: name.to_owned(),
            cols,
        })
    }
    fn col_def(&mut self, pos: usize) -> Result<syntax::ColumnDefinition, ParseError> {
        let name = self.expect(TokenType::IDENTIFIER)?.lexeme.to_string();
        let typ = match self
            .matches(
                |t| matches!(t.typ, TokenType::INTEGER | TokenType::TEXT),
                "invalid column type (expected INTEGER or TEXT)",
            )?
            .typ
        {
            TokenType::INTEGER => ColType::INTEGER,
            TokenType::TEXT => ColType::TEXT,
            v => unreachable!("ehiehiehi che è sta roba {v:?}"),
        };
        let mut constraint = Vec::new();
        while let Some(c) = self.column_constraint()? {
            constraint.push(c);
        }

        Ok(syntax::ColumnDefinition {
            name,
            position: pos,
            typ,
            constraint,
        })
    }
    fn column_constraint(&mut self) -> Result<Option<ColumnConstraint>, ParseError> {
        if let Ok(next) = self.matches(
            |t| {
                matches!(
                    t.typ,
                    TokenType::PRIMARY | TokenType::NOT | TokenType::UNIQUE
                )
            },
            "should never print this",
        ) {
            match next.typ {
                TokenType::PRIMARY => {
                    self.expect(TokenType::KEY)?;
                    let mut asc = true;
                    if let Ok(t) =
                        self.matches(|t| matches!(t.typ, TokenType::ASC | TokenType::DESC), "Meh")
                    {
                        asc = t.typ == TokenType::ASC;
                    }
                    let conflict = self.conflict_clause()?;
                    let autoinc = self
                        .matches(|t| t.typ == TokenType::AUTOINCREMENT, "Mah")
                        .is_ok();
                    Ok(Some(ColumnConstraint::Pk {
                        asc,
                        autoinc,
                        conflict,
                    }))
                }
                TokenType::NOT => {
                    self.expect(TokenType::NULL)?;
                    Ok(Some(ColumnConstraint::NotNull(self.conflict_clause()?)))
                }
                TokenType::UNIQUE => Ok(Some(ColumnConstraint::Unique(self.conflict_clause()?))),
                _ => unreachable!("Ooioioioi"),
            }
        } else {
            Ok(None)
        }
    }
    fn conflict_clause(&mut self) -> Result<Option<ConflictClause>, ParseError> {
        if self.matches(|t| t.typ == TokenType::ON, "oi").is_err() {
            return Ok(None);
        }
        self.expect(TokenType::CONFLICT)?;
        if let Ok(tok) = self.matches(
            |t| {
                matches!(
                    t.typ,
                    TokenType::ROLLBACK
                        | TokenType::ABORT
                        | TokenType::FAIL
                        | TokenType::IGNORE
                        | TokenType::REPLACE
                )
            },
            "yolo",
        ) {
            match tok.typ {
                TokenType::ROLLBACK => Ok(Some(ConflictClause::Rollback)),
                TokenType::ABORT => Ok(Some(ConflictClause::Abort)),
                TokenType::FAIL => Ok(Some(ConflictClause::Fail)),
                TokenType::IGNORE => Ok(Some(ConflictClause::Ignore)),
                TokenType::REPLACE => Ok(Some(ConflictClause::Replace)),
                _ => unreachable!("Mo'"),
            }
        } else {
            Ok(None)
        }
    }

    // NOTE: Operator precedence is taken from https://learn.microsoft.com/en-us/sql/t-sql/language-elements/operator-precedence-transact-sql?view=sql-server-ver16
    // plus I'm actively ignoring assignment
    fn expression(&mut self) -> Result<syntax::Expr, ParseError> {
        self.logic_or()
    }

    // FIXME: I'm convinced this could be rewritten in a better way
    fn logic_or(&mut self) -> Result<syntax::Expr, ParseError> {
        let mut expr = self.logic_and()?;
        while self
            .matches(|typ| matches!(typ.typ, TokenType::OR), "expected OR")
            .is_ok()
        {
            let right = self.logic_and()?;
            expr = syntax::Expr::Binary {
                left: Box::new(expr),
                right: Box::new(right),
                operator: Operator::Or,
            };
        }
        Ok(expr)
    }
    fn logic_and(&mut self) -> Result<syntax::Expr, ParseError> {
        let mut expr = self.equality()?;
        while self
            .matches(|typ| matches!(typ.typ, TokenType::AND), "expected AND")
            .is_ok()
        {
            let right = self.equality()?;
            expr = syntax::Expr::Binary {
                left: Box::new(expr),
                right: Box::new(right),
                operator: Operator::And,
            };
        }
        Ok(expr)
    }

    fn equality(&mut self) -> Result<syntax::Expr, ParseError> {
        let mut expr = self.comparison()?;
        while let Ok(o) = self.matches(
            |typ| matches!(typ.typ, TokenType::EQUALS | TokenType::NOTEQUALS),
            "missing equality operator",
        ) {
            let t = o.typ;
            let right = self.comparison()?;
            expr = syntax::Expr::Binary {
                left: Box::new(expr),
                right: Box::new(right),
                operator: match t {
                    TokenType::NOTEQUALS => Operator::Notequals,
                    TokenType::EQUALS => Operator::Equals,
                    _ => unreachable!("oioi"),
                },
            };
        }
        Ok(expr)
    }

    fn comparison(&mut self) -> Result<syntax::Expr, ParseError> {
        let mut expr = self.term()?;
        while let Ok(o) = self.matches(
            |typ| {
                matches!(
                    typ.typ,
                    TokenType::LESS | TokenType::LESSEQ | TokenType::GREATER | TokenType::GREATEREQ
                )
            },
            "missing equality operator",
        ) {
            let t = o.typ;
            let right = self.term()?;
            expr = syntax::Expr::Binary {
                left: Box::new(expr),
                right: Box::new(right),
                operator: match t {
                    TokenType::LESSEQ => Operator::LessEq,
                    TokenType::LESS => Operator::Less,
                    TokenType::GREATEREQ => Operator::GreaterEq,
                    TokenType::GREATER => Operator::Greater,
                    _ => unreachable!("oioi"),
                },
            };
            //println!("{expr:?}");
        }
        Ok(expr)
    }
    fn term(&mut self) -> Result<syntax::Expr, ParseError> {
        let mut expr = self.factor()?;
        while let Ok(o) = self.matches(
            |typ| matches!(typ.typ, TokenType::PLUS | TokenType::MINUS),
            "missing equality operator",
        ) {
            let t = o.typ;
            let right = self.factor()?;
            expr = syntax::Expr::Binary {
                left: Box::new(expr),
                right: Box::new(right),
                operator: match t {
                    TokenType::PLUS => Operator::Plus,
                    TokenType::MINUS => Operator::Minus,
                    _ => unreachable!("oioi"),
                },
            };
        }
        Ok(expr)
    }
    fn factor(&mut self) -> Result<syntax::Expr, ParseError> {
        let mut expr = self.unary()?;
        while let Ok(o) = self.matches(
            |typ| matches!(typ.typ, TokenType::SLASH | TokenType::ASTERISK),
            "missing equality operator",
        ) {
            let t = o.typ;
            let right = self.unary()?;
            expr = syntax::Expr::Binary {
                left: Box::new(expr),
                right: Box::new(right),
                operator: match t {
                    TokenType::SLASH => Operator::Slash,
                    TokenType::ASTERISK => Operator::Asterisk,
                    _ => unreachable!("oioi"),
                },
            };
        }
        Ok(expr)
    }
    fn unary(&mut self) -> Result<syntax::Expr, ParseError> {
        if let Ok(o) = self.matches(
            |t| matches!(t.typ, TokenType::BANG | TokenType::MINUS),
            "expected either ! or -",
        ) {
            let t = o.typ;
            Ok(Expr::Unary {
                operator: match t {
                    TokenType::MINUS => Operator::Minus,
                    TokenType::BANG => Operator::Bang,
                    _ => unreachable!("noooo"),
                },
                expr: Box::new(self.unary()?),
            })
        } else {
            self.call()
        }
    }

    fn call(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.primary()?;
        Ok(loop {
            if self
                .matches(|t| matches!(t.typ, TokenType::OPENP), "aaa")
                .is_ok()
            {
                expr = self.finish_call(expr)?;
            } else {
                break expr;
            }
        })
    }

    // NOTE: tihs is absolutely not my bet code
    fn finish_call(&mut self, expr: Expr) -> Result<Expr, ParseError> {
        let res = if let Expr::Identifier { value } = expr {
            // NOTE: NON PENSO ESISTANO FUNZIONI SENZA ARGOMENTI IN SQL(cioè si ma non hanno le
            // parentesi e per ora non ci penso 💅 )
            if TokenType::ASTERISK == self.peek().ok_or(ParseError::NoInput)?.typ {
                self.step().unwrap();
                Ok(Expr::Function {
                    name: value,
                    args: FunctionArg::Star,
                })
            } else {
                let mut args = Vec::new();
                loop {
                    let expr = self.expression()?;
                    args.push(expr);
                    if self.matches(|t| t.typ == TokenType::COMMA, "yeet").is_err() {
                        break;
                    }
                }
                Ok(Expr::Function {
                    name: value,
                    args: FunctionArg::Args(args),
                })
            }
        } else {
            Err(ParseError::CustomError(
                "function name cannot be expression",
            ))
        };
        self.expect(TokenType::CLOSEP)?;
        res
    }

    fn primary(&mut self) -> Result<Expr, ParseError> {
        let p = self.step().ok_or(ParseError::NoInput)?;
        match p.typ {
            TokenType::IDENTIFIER => Ok(Expr::Identifier {
                value: p.lexeme.to_owned(),
            }),
            // TODO: I guess this could use a little bit more...oopmh
            TokenType::TRUE => Ok(Expr::Literal {
                value: syntax::DbValue::Bool(true),
            }),
            TokenType::FALSE => Ok(Expr::Literal {
                value: syntax::DbValue::Bool(false),
            }),
            TokenType::NULL => Ok(Expr::Literal {
                value: syntax::DbValue::Null,
            }),
            TokenType::NUMBER => {
                Ok(Expr::Literal {
                    value: if p.lexeme.contains('.') {
                        syntax::DbValue::Float(p.lexeme.parse::<f64>().map_err(|_| {
                            ParseError::CustomError("could not parse number into i64")
                        })?)
                    } else {
                        syntax::DbValue::Integer(i64::from_str_radix(&p.lexeme, 10).map_err(
                            |_| ParseError::CustomError("could not parse number into i64"),
                        )?)
                    },
                })
            }
            TokenType::STRING => Ok(Expr::Literal {
                value: syntax::DbValue::Text(p.lexeme.to_owned()),
            }),
            TokenType::OPENP => {
                let expr = self.expression()?;
                self.expect(TokenType::CLOSEP)?;
                Ok(Expr::Grouping {
                    expr: Box::new(expr),
                })
            }
            _ => Err(ParseError::InvalidKeyword(p.lexeme.to_owned())),
        }
    }
}

#[cfg(test)]
mod test {
    use super::syntax::*;
    use super::*;
    use crate::sql::lexer::*;

    #[test]
    fn test_parse() {
        let mut p = Parser::new(
            &[
                Token {
                    typ: TokenType::CREATE,
                    lexeme: "CREATE",
                },
                Token {
                    typ: TokenType::TABLE,
                    lexeme: "TABLE",
                },
                Token {
                    typ: TokenType::IDENTIFIER,
                    lexeme: "gatto",
                },
                Token {
                    typ: TokenType::OPENP,
                    lexeme: "(",
                },
                Token {
                    typ: TokenType::IDENTIFIER,
                    lexeme: "miao",
                },
                Token {
                    typ: TokenType::TEXT,
                    lexeme: "TEXT",
                },
                Token {
                    typ: TokenType::COMMA,
                    lexeme: ",",
                },
                Token {
                    typ: TokenType::IDENTIFIER,
                    lexeme: "id",
                },
                Token {
                    typ: TokenType::INTEGER,
                    lexeme: "INTEGER",
                },
                Token {
                    typ: TokenType::PRIMARY,
                    lexeme: "PRIMARY",
                },
                Token {
                    typ: TokenType::KEY,
                    lexeme: "KEY",
                },
                Token {
                    typ: TokenType::CLOSEP,
                    lexeme: ")",
                },
                Token {
                    typ: TokenType::SEMICOLON,
                    lexeme: ";",
                },
                Token {
                    typ: TokenType::EOF,
                    lexeme: "",
                },
            ], //scan("CREATE TABLE gatto(\n miao TEXT,\n id INTEGER PRIMARY KEY \n);"),
        );
        /*let mut p = Parser::new(&[
            Token {
                typ: TokenType::SELECT,
                lexeme: "SELECT",
            },
            Token {
                typ: TokenType::IDENTIFIER,
                lexeme: "miao",
            },
            Token {
                typ: TokenType::FROM,
                lexeme: "FROM",
            },
            Token {
                typ: TokenType::IDENTIFIER,
                lexeme: "gatto",
            },
            Token {
                typ: TokenType::EOF,
                lexeme: "",
            },
        ]);*/
        let r = p.scan();
        println!("{r:?}");
        assert_eq!(
            r,
            [Statement::Create(CreateStatement::Table {
                name: "gatto".to_owned(),
                cols: vec![
                    ColumnDefinition {
                        name: "miao".to_owned(),
                        position: 0,
                        typ: ColType::TEXT,
                        constraint: Vec::new(),
                    },
                    ColumnDefinition {
                        name: "id".to_owned(),
                        position: 1,
                        typ: ColType::INTEGER,
                        constraint: vec![ColumnConstraint::Pk {
                            asc: true,
                            autoinc: false,
                            conflict: None
                        }]
                    }
                ]
            })]
        );
    }
    #[test]
    fn test_simple_select() {
        let select = "SELECT * FROM gatito";
        let p = Parser::new(&scan(select)).scan();
        println!("{select}\n{p:?}");
        let count = "SELECT COUNT(*) FROM gatito";
        let p = Parser::new(&scan(count)).scan();
        println!("{count}\n{p:?}");
        let cols = "SELECT colore, nome FROM gatito";
        let p = Parser::new(&scan(cols)).scan();
        println!("{cols}\n{p:?}");
        let filter = "SELECT colore, nome FROM gatito WHERE colore = 'black'";
        let p = Parser::new(&scan(filter)).scan();
        println!("{filter}\n{p:?}");
        let filter_expr = "SELECT colore, nome FROM gatito WHERE colore = TRUE ";
        let p = Parser::new(&scan(filter_expr)).scan();
        println!("{filter_expr}\n{p:?}");
        let filter_expr_2 = "SELECT colore, nome FROM gatito WHERE colore = 12 + 2 ";
        let p = Parser::new(&scan(filter_expr_2)).scan();
        println!("{filter_expr_2}\n{p:?}");
        let filter_expr_3 = "SELECT colore, nome FROM gatito WHERE colore = 12 + 2 + (12 *2)/4 ";
        let p = Parser::new(&scan(filter_expr_3)).scan();
        println!("{filter_expr_3}\n{p:?}");
    }
    #[test]
    fn test_schema_table() {
        let schema_def = "CREATE TABLE sqlite_schema(
  type text,
  name text,
  tbl_name text,
  rootpage integer,
  sql text
);";
        let p = Parser::new(&scan(schema_def)).scan();
        println!("{schema_def}\n{p:?}");
        let schema_def = "CREATE TABLE sqlite_schema(
  type text,
  name text,
  tbl_name text,
  rootpage integer,
  sql text
);
CREATE TABLE sqlite_schema(
  type text,
  name text,
  tbl_name text,
  rootpage integer,
  sql text
);";
        let p = Parser::new(&scan(schema_def)).scan();
        println!("{schema_def}\n{p:?}");
    }
}
