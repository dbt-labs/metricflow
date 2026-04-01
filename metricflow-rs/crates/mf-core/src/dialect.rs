use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlDialect {
    DuckDB,
    BigQuery,
    Snowflake,
    Redshift,
    Postgres,
    Databricks,
    Trino,
}

impl fmt::Display for SqlDialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::DuckDB => "DuckDB",
            Self::BigQuery => "BigQuery",
            Self::Snowflake => "Snowflake",
            Self::Redshift => "Redshift",
            Self::Postgres => "Postgres",
            Self::Databricks => "Databricks",
            Self::Trino => "Trino",
        };
        write!(f, "{s}")
    }
}
