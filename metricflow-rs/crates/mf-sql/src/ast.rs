/// A SQL expression (column ref, function call, literal, etc.)
#[derive(Debug, Clone)]
pub enum SqlExpr {
    /// A column reference: `table_alias.column_name`
    ColumnRef {
        table_alias: String,
        column_name: String,
    },
    /// An aggregate function: `SUM(expr)`, `COUNT(DISTINCT expr)`, etc.
    AggregateFunction {
        function: String,
        arg: Box<SqlExpr>,
        distinct: bool,
    },
    /// A scalar function call: `DATE_TRUNC('day', expr)`
    FunctionCall {
        function: String,
        args: Vec<SqlExpr>,
    },
    /// A raw SQL literal or expression: `1`, `'day'`, etc.
    Literal(String),
    /// An aliased expression: `expr AS alias`
    Alias { expr: Box<SqlExpr>, alias: String },
}

/// A SQL FROM source.
#[derive(Debug, Clone)]
pub enum SqlFrom {
    /// A table reference: `schema.table`
    Table { table: String, alias: String },
    /// A subquery: `(SELECT ...) AS alias`
    Subquery {
        query: Box<SqlSelect>,
        alias: String,
    },
}

/// A SQL JOIN clause.
#[derive(Debug, Clone)]
pub struct SqlJoin {
    pub join_type: String, // "LEFT OUTER JOIN", "INNER JOIN", etc.
    pub source: SqlFrom,
    pub on: SqlExpr,
}

/// A SQL ORDER BY item.
#[derive(Debug, Clone)]
pub struct SqlOrderBy {
    pub expr: SqlExpr,
    pub descending: bool,
}

/// A complete SQL SELECT statement.
#[derive(Debug, Clone)]
pub struct SqlSelect {
    pub select_columns: Vec<SqlExpr>,
    pub from: SqlFrom,
    pub joins: Vec<SqlJoin>,
    pub where_clause: Option<SqlExpr>,
    pub group_by: Vec<SqlExpr>,
    pub order_by: Vec<SqlOrderBy>,
    pub limit: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_select_construction() {
        let select = SqlSelect {
            select_columns: vec![SqlExpr::Alias {
                expr: Box::new(SqlExpr::AggregateFunction {
                    function: "SUM".into(),
                    arg: Box::new(SqlExpr::ColumnRef {
                        table_alias: "subq_0".into(),
                        column_name: "__bookings".into(),
                    }),
                    distinct: false,
                }),
                alias: "bookings".into(),
            }],
            from: SqlFrom::Table {
                table: "demo.fct_bookings".into(),
                alias: "subq_0".into(),
            },
            joins: vec![],
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };
        assert_eq!(select.select_columns.len(), 1);
    }
}
