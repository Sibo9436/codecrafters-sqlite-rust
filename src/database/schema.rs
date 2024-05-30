const SCHEMA_DEF: &'static str = "CREATE TABLE sqlite_schema(
  type text,
  name text,
  tbl_name text,
  rootpage integer,
  sql text
);";
pub(crate) struct Schema {
    rows: Vec<SchemaRow>,
}
pub(crate) enum ColumnType {
    Integer,
    Float,
    String,
    Blob,
}
pub(crate) trait RowDef {
    fn records() -> Vec<ColumnType>;
}

pub(crate) struct SchemaRow {
    obj_type: String,
    name: String,
    tbl_name: String,
    rootpage: i64,
    sql: String,
}

impl RowDef for SchemaRow {
    fn records() -> Vec<ColumnType> {
        vec![
            ColumnType::String,
            ColumnType::String,
            ColumnType::String,
            ColumnType::Integer,
            ColumnType::String,
        ]
    }
}
