use std::{
    fmt::Write,
    ops::{Add, Div, Mul, Neg, Not, Sub},
};

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub(crate) enum DbValue {
    Bool(bool),
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}
#[derive(Debug, PartialEq)]
pub(crate) enum Statement {
    Create(CreateStatement),
    Select(SelectStatement),
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CreateStatement {
    Table {
        name: String,
        cols: Vec<ColumnDefinition>,
    },
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct ColumnDefinition {
    pub name: String,
    pub position: usize,
    pub typ: ColType,
    pub constraint: Vec<ColumnConstraint>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum ColumnConstraint {
    Pk {
        asc: bool,
        autoinc: bool,
        conflict: Option<ConflictClause>,
    },
    NotNull(Option<ConflictClause>),
    Unique(Option<ConflictClause>),
    //Default()
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum ConflictClause {
    Rollback,
    Abort,
    Fail,
    Ignore,
    Replace,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum ColType {
    INTEGER,
    TEXT,
}
#[derive(Debug, PartialEq)]
pub(crate) struct SelectStatement {
    pub from: String,
    pub fields: Vec<Expr>,
    pub filter: Option<Expr>,
}
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Expr {
    Identifier {
        value: String,
    },
    Literal {
        value: DbValue,
    },

    Binary {
        left: Box<Expr>,
        right: Box<Expr>,
        operator: Operator,
    },
    Unary {
        operator: Operator,
        expr: Box<Expr>,
    },
    Function {
        name: String,
        args: FunctionArg,
    },
    Grouping {
        expr: Box<Expr>,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum Operator {
    Plus,
    Minus,
    Equals,
    Bang,
    Notequals,
    // NOTE: we use the inverse for <= and >= (a<=b means b > a)
    Less,
    Greater,
    LessEq,
    GreaterEq,
    Asterisk,
    Slash,
    Or,
    And,
    Not,
}
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum FunctionArg {
    Star,
    Args(Vec<Expr>),
}

pub(crate) trait Visit<T> {
    fn visit_expr(&mut self, e: &Expr) -> T;
    // NOTE: non penso siano necessari
    //fn visit_create_statement(&mut self, e: &CreateStatement) -> T;
    //fn visit_select_statement(&mut self, e: &SelectStatement) -> T;
    fn visit_statement(&mut self, e: &Statement) -> T;
    fn visit_column_definition(&mut self, e: &ColumnDefinition) -> T;
}

pub(crate) struct AstPrinter(pub String);
impl AstPrinter {
    pub fn print(&mut self, s: &Statement) {
        self.visit_statement(s);
    }
}

impl Visit<()> for AstPrinter {
    fn visit_expr(&mut self, e: &Expr) -> () {
        match e {
            Expr::Identifier { value } => println!("{}val:[{value}]", self.0),
            Expr::Literal { value } => println!("{}val:[{value:?}]", self.0),
            Expr::Binary {
                left,
                right,
                operator,
            } => {
                self.0.push('\t');
                self.visit_expr(left);
                self.0.pop();
                println!("{}op:{:?}", self.0, operator);
                self.0.push('\t');
                self.visit_expr(right);
                self.0.pop();
            }
            Expr::Unary { operator, expr } => {
                println!("{}op:{:?}", self.0, operator);
                self.0.push('\t');
                self.visit_expr(expr);
                self.0.pop();
            }
            Expr::Function { name, args } => {
                println!("{}function: {name}", self.0);
                self.0.push('\t');
                match args {
                    FunctionArg::Star => println!("*"),
                    FunctionArg::Args(args) => {
                        for e in args {
                            self.visit_expr(e);
                        }
                    }
                }
                self.0.pop();
            }
            Expr::Grouping { expr } => {
                println!("{}(", self.0);
                self.0.push('\t');
                self.visit_expr(expr);
                self.0.pop();
                println!("{})", self.0);
            }
        }
    }

    fn visit_statement(&mut self, e: &Statement) -> () {
        match e {
            Statement::Create(CreateStatement::Table { name, cols }) => {
                println!("{}create table {name}", self.0);
                self.0.push('\t');
                for col in cols {
                    self.visit_column_definition(col);
                }
                self.0.pop();
            }

            Statement::Select(SelectStatement {
                from,
                fields,
                filter,
            }) => {
                println!("{}select from {from}:", self.0);
                self.0.push('\t');
                if fields.is_empty() {
                    println!("{}all fields", self.0);
                } else {
                    for field in fields {
                        self.visit_expr(field);
                    }
                }
                if let Some(f) = filter {
                    self.visit_expr(f)
                }
                self.0.pop();
            }
        }
    }

    fn visit_column_definition(&mut self, e: &ColumnDefinition) -> () {
        println!("{}column: {} {:?}", self.0, e.name, e.typ);
        for c in &e.constraint {
            // NOTE: molto simpatico il fatto che il pattern matching mi permetta di evitare
            // di creare le funzioni "accept"
            match c {
                ColumnConstraint::Pk { .. } => println!("{}pk", self.0),
                ColumnConstraint::NotNull(_) => println!("{}NotNull", self.0),
                ColumnConstraint::Unique(_) => println!("{}Unique", self.0),
            }
        }
    }
}

impl Add for DbValue {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        match (&self, &rhs) {
            (DbValue::Null, DbValue::Null) => self,
            (DbValue::Integer(f), DbValue::Integer(s)) => DbValue::Integer(f + s),
            (DbValue::Text(f), DbValue::Text(s)) => DbValue::Text(format!("{f}{s}")),
            //(DbValue::Blob(_), DbValue::Blob(_)) => todo!(),
            // TODO: tutti sti panic
            _ => panic!("adding different dbvalues is not supported"),
        }
    }
}

impl Not for DbValue {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            DbValue::Bool(b) => Self::Bool(!b),
            DbValue::Integer(i) => Self::Integer(!i),
            _ => panic!("ajo"),
        }
    }
}

impl Neg for DbValue {
    type Output = Self;

    fn neg(self) -> Self::Output {
        match self {
            DbValue::Integer(i) => Self::Integer(-i),
            _ => panic!("Nooo"),
        }
    }
}
impl Sub for DbValue {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        match (&self, &rhs) {
            (DbValue::Integer(f), DbValue::Integer(s)) => Self::Integer(f - s),
            _ => unimplemented!("subtraction meh"),
        }
    }
}

impl Mul for DbValue {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        match (&self, &rhs) {
            (DbValue::Integer(f), DbValue::Integer(s)) => Self::Integer(f * s),
            _ => unimplemented!("mult meh"),
        }
    }
}
impl Div for DbValue {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        match (&self, &rhs) {
            (DbValue::Integer(f), DbValue::Integer(s)) => Self::Integer(f / s),
            _ => unimplemented!("div meh"),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{sql::syntax::DbValue, *};

    use self::sql::{lexer, parser};

    use super::AstPrinter;
    #[test]
    fn test_print() {
        let mut printer = AstPrinter("".to_owned());
        let stmt = "CREATE TABLE ciao ( id INTEGER PRIMARY KEY, name TEXT)";
        let tree = parser::Parser::new(&lexer::scan(stmt)).scan();
        for s in &tree {
            printer.print(s);
        }
        let stmt = "SELECT name, sum(gatito, id), id from kitty where age * 2 - (15 - 3) >= 10";
        let tree = parser::Parser::new(&lexer::scan(stmt)).scan();
        for s in &tree {
            printer.print(s);
        }
    }
    #[test]
    fn test_equality() {
        assert_eq!(
            true,
            DbValue::Text("ciao".to_owned()) == DbValue::Text("ciao".to_owned())
        );
        assert_eq!(
            false,
            DbValue::Text("ciao".to_owned()) != DbValue::Text("ciao".to_owned())
        );
        assert_eq!(
            true,
            DbValue::Text("ciao".to_owned()) != DbValue::Text("Ciao".to_owned())
        );
    }
}
