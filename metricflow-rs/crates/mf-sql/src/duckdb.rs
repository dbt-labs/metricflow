use crate::ast::*;
use crate::render::{DefaultRenderer, SqlRenderer};

/// DuckDB-specific SQL renderer.
/// Inherits most behavior from DefaultRenderer, overrides DuckDB-specific functions.
pub struct DuckDbRenderer;

impl SqlRenderer for DuckDbRenderer {
    fn render(&self, select: &SqlSelect) -> String {
        // Delegate to default renderer — DuckDB differences are in expression rendering
        DefaultRenderer.render(select)
    }

    fn render_expr(&self, expr: &SqlExpr) -> String {
        match expr {
            SqlExpr::FunctionCall { function, args } if function == "DATE_TRUNC" => {
                // DuckDB: DATE_TRUNC('grain', expr) — same as ANSI, but ensure lowercase grain
                let args_str: Vec<String> = args.iter().map(|a| self.render_expr(a)).collect();
                format!("DATE_TRUNC({})", args_str.join(", "))
            }
            // All other expressions use default rendering
            _ => DefaultRenderer.render_expr(expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duckdb_renders_date_trunc() {
        let expr = SqlExpr::FunctionCall {
            function: "DATE_TRUNC".into(),
            args: vec![
                SqlExpr::Literal("'day'".into()),
                SqlExpr::ColumnRef {
                    table_alias: "src".into(),
                    column_name: "ds".into(),
                },
            ],
        };
        let renderer = DuckDbRenderer;
        assert_eq!(renderer.render_expr(&expr), "DATE_TRUNC('day', src.ds)");
    }
}
