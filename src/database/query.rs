use std::{collections::HashMap, slice::SliceIndex, thread::current};

use itertools::Itertools;

use crate::sql::syntax::{
    AstPrinter, ColType, ColumnConstraint, ColumnDefinition, DbValue, Expr, Visit,
};

use super::expression::{precompile_expr, Precompile, RowValue, RunnableExpr};

#[derive(Debug)]
pub(crate) struct Column {
    col: ColumnDefinition,
    active: bool,
}
#[derive(Debug)]
pub(crate) struct Table {
    columns: Vec<Column>,
    rows: Vec<Row>,
}

#[derive(Debug)]
pub(crate) struct Row {
    id: DbValue,
    row: Vec<DbValue>,
}

// PERF: this seems extremely inefficient but we'll fiddle with it later

pub(crate) trait QueryOperation {
    fn apply(&mut self, t: Table) -> Table;
}
impl Table {
    fn apply<T: QueryOperation>(self, mut q: T) -> Self {
        q.apply(self)
    }
}

pub(crate) struct QueryFilter {
    expr: Box<RunnableExpr>,
}

pub(crate) struct RowEntry<'a> {
    row: &'a Row,
    cols: &'a [&'a ColumnDefinition],
}
impl<'a> RowValue for RowEntry<'a> {
    fn column(&self, name: &str) -> &DbValue {
        for col in self.cols {
            if col.name == name {
                if col.typ == ColType::INTEGER
                    && col
                        .constraint
                        .iter()
                        .any(|c| matches!(c, ColumnConstraint::Pk { .. }))
                {
                    return &self.row.id;
                } else {
                    return &self.row.row[col.position];
                }
            }
        }
        &DbValue::Null
    }
}

impl QueryFilter {
    pub(crate) fn new(expr: Expr) -> Self {
        Self {
            expr: expr.precompile(),
        }
    }
}
impl QueryOperation for QueryFilter {
    fn apply(&mut self, mut t: Table) -> Table {
        let cols: Vec<&ColumnDefinition> = t.columns.iter().map(|c| &c.col).collect();
        t.rows = t
            .rows
            .into_iter()
            .filter(|row| {
                let access = RowEntry {
                    row: &row,
                    cols: &cols,
                };
                if let DbValue::Bool(b) = (self.expr)(&access) {
                    b
                } else {
                    false
                }
            })
            .collect();
        t
    }
}

pub(crate) struct QuerySelect {
    col_extractors: Vec<Box<RunnableExpr>>,
    col_names: Vec<String>,
}

impl QuerySelect {
    pub(crate) fn new(col_extractors: Vec<Expr>) -> Self {
        Self {
            col_names: col_extractors
                .iter()
                .map(|e| {
                    let mut p = AstPrinter(String::new());
                    p.visit_expr(e);
                    p.0
                })
                .collect(),
            col_extractors: col_extractors.into_iter().map(|e| e.precompile()).collect(),
        }
    }
}

impl QueryOperation for QuerySelect {
    fn apply(&mut self, mut t: Table) -> Table {
        let cols: Vec<&ColumnDefinition> = t.columns.iter().map(|c| &c.col).collect();
        let rows: Vec<Row> = t
            .rows
            .into_iter()
            .map(|row| {
                let access = RowEntry {
                    row: &row,
                    cols: &cols,
                };
                Row {
                    id: row.id.clone(),
                    row: self
                        .col_extractors
                        .iter()
                        .map(|ext| (ext)(&access))
                        .collect(),
                }
            })
            .collect();

        // NOTE: Sta roba non mi piace per nulla
        t.columns = self
            .col_names
            .iter()
            .enumerate()
            .map(|(i, c)| ColumnDefinition {
                name: c.to_owned(),
                position: i,
                // FIXME: Qui dovrei tipo precalcolarlo credo
                typ: ColType::INTEGER,
                constraint: vec![],
            })
            .map(|c| Column {
                col: c,
                active: true,
            })
            .collect();
        t.rows = rows;
        t
    }
}
