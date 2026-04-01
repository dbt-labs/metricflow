use crate::ast::*;
use mf_core::dialect::SqlDialect;

const INDENT: &str = "  ";

/// Trait for rendering SQL AST to a dialect-specific string.
pub trait SqlRenderer {
    fn render(&self, select: &SqlSelect) -> String;
    fn render_expr(&self, expr: &SqlExpr) -> String;
    fn quote_identifier(&self, name: &str) -> String {
        name.to_string() // default: no quoting
    }
}

/// Default ANSI SQL renderer.
pub struct DefaultRenderer;

impl SqlRenderer for DefaultRenderer {
    fn render(&self, select: &SqlSelect) -> String {
        let mut parts = Vec::new();

        // SELECT
        let cols: Vec<String> = select
            .select_columns
            .iter()
            .map(|c| self.render_expr(c))
            .collect();
        if cols.is_empty() {
            parts.push("SELECT\n  *".into());
        } else {
            let first = format!("SELECT\n{INDENT}{}", cols[0]);
            let rest: Vec<String> = cols[1..].iter().map(|c| format!("{INDENT}, {c}")).collect();
            let mut select_parts = vec![first];
            select_parts.extend(rest);
            parts.push(select_parts.join("\n"));
        }

        // FROM
        parts.push(self.render_from(&select.from));

        // JOINs
        for join in &select.joins {
            parts.push(self.render_join(join));
        }

        // WHERE
        if let Some(where_clause) = &select.where_clause {
            parts.push(format!("WHERE\n{INDENT}{}", self.render_expr(where_clause)));
        }

        // GROUP BY
        if !select.group_by.is_empty() {
            let group_cols: Vec<String> = select
                .group_by
                .iter()
                .map(|g| self.render_expr(g))
                .collect();
            let first = format!("GROUP BY\n{INDENT}{}", group_cols[0]);
            let rest: Vec<String> = group_cols[1..]
                .iter()
                .map(|c| format!("{INDENT}, {c}"))
                .collect();
            let mut group_parts = vec![first];
            group_parts.extend(rest);
            parts.push(group_parts.join("\n"));
        }

        // ORDER BY
        if !select.order_by.is_empty() {
            let order_items: Vec<String> = select
                .order_by
                .iter()
                .map(|o| {
                    let expr = self.render_expr(&o.expr);
                    if o.descending {
                        format!("{expr} DESC")
                    } else {
                        expr
                    }
                })
                .collect();
            parts.push(format!("ORDER BY {}", order_items.join(", ")));
        }

        // LIMIT
        if let Some(limit) = select.limit {
            parts.push(format!("LIMIT {limit}"));
        }

        parts.join("\n")
    }

    fn render_expr(&self, expr: &SqlExpr) -> String {
        match expr {
            SqlExpr::ColumnRef { table_alias, column_name } => {
                format!("{table_alias}.{column_name}")
            }
            SqlExpr::AggregateFunction { function, arg, distinct } => {
                let arg_str = self.render_expr(arg);
                if *distinct {
                    format!("{function}(DISTINCT {arg_str})")
                } else {
                    format!("{function}({arg_str})")
                }
            }
            SqlExpr::FunctionCall { function, args } => {
                let args_str: Vec<String> = args.iter().map(|a| self.render_expr(a)).collect();
                format!("{function}({})", args_str.join(", "))
            }
            SqlExpr::Literal(s) => s.clone(),
            SqlExpr::Alias { expr, alias } => {
                format!("{} AS {alias}", self.render_expr(expr))
            }
        }
    }
}

impl DefaultRenderer {
    fn render_from(&self, from: &SqlFrom) -> String {
        match from {
            SqlFrom::Table { table, alias } => {
                format!("FROM\n{INDENT}{table} {alias}")
            }
            SqlFrom::Subquery { query, alias } => {
                let inner = self.render(query);
                let indented = inner
                    .lines()
                    .map(|l| format!("{INDENT}{INDENT}{l}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("FROM (\n{indented}\n{INDENT}) {alias}")
            }
        }
    }

    fn render_join(&self, join: &SqlJoin) -> String {
        let source = match &join.source {
            SqlFrom::Table { table, alias } => format!("{INDENT}{table} {alias}"),
            SqlFrom::Subquery { query, alias } => {
                let inner = self.render(query);
                let indented = inner
                    .lines()
                    .map(|l| format!("{INDENT}{INDENT}{l}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{INDENT}(\n{indented}\n{INDENT}) {alias}")
            }
        };
        let on = self.render_expr(&join.on);
        format!("{}\n{source}\nON\n{INDENT}{on}", join.join_type)
    }
}

/// Return the appropriate renderer for a dialect.
pub fn renderer_for_dialect(dialect: SqlDialect) -> Box<dyn SqlRenderer> {
    match dialect {
        SqlDialect::DuckDB => Box::new(crate::duckdb::DuckDbRenderer),
        // All other dialects fall back to default for now
        _ => Box::new(DefaultRenderer),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_simple_select() {
        let select = SqlSelect {
            select_columns: vec![
                SqlExpr::Alias {
                    expr: Box::new(SqlExpr::AggregateFunction {
                        function: "SUM".into(),
                        arg: Box::new(SqlExpr::ColumnRef {
                            table_alias: "subq_0".into(),
                            column_name: "__bookings".into(),
                        }),
                        distinct: false,
                    }),
                    alias: "bookings".into(),
                },
            ],
            from: SqlFrom::Subquery {
                query: Box::new(SqlSelect {
                    select_columns: vec![
                        SqlExpr::Alias {
                            expr: Box::new(SqlExpr::Literal("1".into())),
                            alias: "__bookings".into(),
                        },
                    ],
                    from: SqlFrom::Table {
                        table: "demo.fct_bookings".into(),
                        alias: "bookings_source_src".into(),
                    },
                    joins: vec![],
                    where_clause: None,
                    group_by: vec![],
                    order_by: vec![],
                    limit: None,
                }),
                alias: "subq_0".into(),
            },
            joins: vec![],
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };

        let renderer = DefaultRenderer;
        let sql = renderer.render(&select);
        assert!(sql.contains("SUM(subq_0.__bookings) AS bookings"));
        assert!(sql.contains("FROM ("));
        assert!(sql.contains("demo.fct_bookings"));
    }

    #[test]
    fn test_render_with_group_by() {
        let select = SqlSelect {
            select_columns: vec![
                SqlExpr::ColumnRef {
                    table_alias: "subq_0".into(),
                    column_name: "metric_time__day".into(),
                },
                SqlExpr::Alias {
                    expr: Box::new(SqlExpr::AggregateFunction {
                        function: "SUM".into(),
                        arg: Box::new(SqlExpr::ColumnRef {
                            table_alias: "subq_0".into(),
                            column_name: "__bookings".into(),
                        }),
                        distinct: false,
                    }),
                    alias: "bookings".into(),
                },
            ],
            from: SqlFrom::Table {
                table: "demo.fct_bookings".into(),
                alias: "subq_0".into(),
            },
            joins: vec![],
            where_clause: None,
            group_by: vec![
                SqlExpr::ColumnRef {
                    table_alias: "subq_0".into(),
                    column_name: "metric_time__day".into(),
                },
            ],
            order_by: vec![],
            limit: None,
        };

        let renderer = DefaultRenderer;
        let sql = renderer.render(&select);
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("metric_time__day"));
    }
}
