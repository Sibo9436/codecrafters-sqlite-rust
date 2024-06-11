use std::collections::HashMap;

use crate::sql::{
    self,
    syntax::{DbValue, Expr, Operator},
};

pub(crate) trait RowValue {
    fn column(&self, name: &str) -> &DbValue;
}

impl RowValue for HashMap<String, DbValue> {
    fn column(&self, name: &str) -> &DbValue {
        self.get(name).unwrap_or(&DbValue::Null)
    }
}

pub(crate) type RunnableExpr = dyn Fn(&dyn RowValue) -> DbValue;

pub(crate) trait Precompile {
    fn precompile(self) -> Box<RunnableExpr>;
}

impl Precompile for Expr {
    fn precompile(self) -> Box<RunnableExpr> {
        match self {
            Expr::Identifier { value } => Box::new(move |row| row.column(&value).clone()),
            Expr::Literal { value } => Box::new(move |_| value.clone()),
            Expr::Binary {
                left,
                right,
                operator,
            } => {
                let left = left.precompile();
                let right = right.precompile();
                let operation: fn(DbValue, DbValue) -> DbValue = match operator {
                    Operator::Plus => |a, b| a + b,
                    Operator::Minus => |a, b| a + b,
                    Operator::Equals => |l, r| DbValue::Bool(l == r),
                    Operator::Notequals => |l, r| {
                        println!("Checking {l:?} against {r:?}");
                        DbValue::Bool(l != r)
                    },
                    Operator::Less => |l, r| DbValue::Bool(l < r),
                    Operator::Greater => |l, r| DbValue::Bool(l > r),
                    Operator::LessEq => |l, r| DbValue::Bool(l <= r),
                    Operator::GreaterEq => |l, r| DbValue::Bool(l >= r),
                    Operator::Asterisk => |a, b| a * b,
                    Operator::Slash => |a, b| a / b,
                    Operator::Or => |a, b| match (a, b) {
                        (DbValue::Bool(a), DbValue::Bool(b)) => DbValue::Bool(a || b),
                        _ => panic!("kill me"),
                    },
                    Operator::And => |a, b| match (a, b) {
                        (DbValue::Bool(a), DbValue::Bool(b)) => DbValue::Bool(a && b),
                        _ => panic!("kill me"),
                    },
                    Operator::Bang | Operator::Not => unreachable!(),
                };
                Box::new(move |row| operation(left(row), right(row)))
            }
            Expr::Unary { operator, expr } => {
                let e = expr.precompile();
                let op: fn(DbValue) -> DbValue = match operator {
                    sql::syntax::Operator::Minus => |v| -v,
                    sql::syntax::Operator::Bang => |v| !v,
                    _ => unimplemented!("what are you trying to do"),
                };
                Box::new(move |row| op(e(row)))
            }

            Expr::Function { .. } => unimplemented!("We are still not able to support functions"),
            Expr::Grouping { expr } => {
                let m = expr.precompile();
                Box::new(move |row| m(row))
            }
        }
    }
}

pub(super) fn precompile_expr<'a>(
    e: &'a Expr,
) -> Box<dyn Fn(&HashMap<String, DbValue>) -> DbValue + 'a> {
    match e {
        Expr::Identifier { value } => Box::new(|map| map.get(value).unwrap().clone()),
        Expr::Literal { value } => Box::new(move |_| value.clone()),
        Expr::Binary {
            left,
            right,
            operator,
        } => precompile_binary(left, right, operator),
        Expr::Unary { operator, expr } => precompile_unary(operator, expr),
        Expr::Function { .. } => unimplemented!(),
        Expr::Grouping { expr } => precompile_expr(expr),
    }
}

pub(super) fn precompile_binary<'a>(
    left: &'a Expr,
    right: &'a Expr,
    operator: &'a sql::syntax::Operator,
) -> Box<dyn Fn(&HashMap<String, DbValue>) -> DbValue + 'a> {
    let left = precompile_expr(left);
    let right = precompile_expr(right);
    let op: fn(DbValue, DbValue) -> DbValue = match operator {
        sql::syntax::Operator::Plus => |l, r| l + r,
        sql::syntax::Operator::Minus => |l, r| l - r,
        sql::syntax::Operator::Equals => |l, r| DbValue::Bool(l == r),
        sql::syntax::Operator::Bang => panic!("nope!"),
        sql::syntax::Operator::Notequals => |l, r| {
            println!("Checking {l:?} against {r:?}");
            DbValue::Bool(l != r)
        },
        sql::syntax::Operator::Less => |l, r| DbValue::Bool(l < r),
        sql::syntax::Operator::Greater => |l, r| DbValue::Bool(l > r),
        sql::syntax::Operator::LessEq => |l, r| DbValue::Bool(l <= r),
        sql::syntax::Operator::GreaterEq => |l, r| DbValue::Bool(l >= r),
        sql::syntax::Operator::Asterisk => |l, r| l * r,
        sql::syntax::Operator::Slash => |l, r| l / r,
        sql::syntax::Operator::Or => |l, r| match (l, r) {
            (DbValue::Bool(l), DbValue::Bool(r)) => DbValue::Bool(l || r),
            _ => unimplemented!("Or of weird values going on"),
        },
        sql::syntax::Operator::And => |l, r| match (l, r) {
            (DbValue::Bool(l), DbValue::Bool(r)) => DbValue::Bool(l && r),
            _ => unimplemented!("AND of weird values going on"),
        },
        sql::syntax::Operator::Not => unimplemented!("NOT is a unary operator"),
    };
    Box::new(move |map| op(left(map), right(map)))
}

pub(super) fn precompile_unary<'a>(
    operator: &'a sql::syntax::Operator,
    expr: &'a Expr,
) -> Box<dyn Fn(&HashMap<String, DbValue>) -> DbValue + 'a> {
    let e = precompile_expr(expr);
    let op: fn(DbValue) -> DbValue = match operator {
        sql::syntax::Operator::Minus => |v| -v,
        sql::syntax::Operator::Bang => |v| !v,
        _ => unimplemented!("what are you trying to do"),
    };
    Box::new(move |map| op(e(map)))
}
