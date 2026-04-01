use clap::{Parser, ValueEnum};
use mf_core::dialect::SqlDialect;
use mf_core::spec::*;
use mf_core::types::TimeGrain;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "mf", about = "MetricFlow query compiler")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Compile a metric query to SQL
    Query {
        /// Path to semantic_manifest.json
        #[arg(long)]
        manifest: PathBuf,

        /// Metric names (comma-separated)
        #[arg(long, value_delimiter = ',')]
        metrics: Vec<String>,

        /// Group-by dimensions (comma-separated, e.g., "metric_time,is_instant")
        #[arg(long, value_delimiter = ',')]
        group_by: Vec<String>,

        /// Time grain
        #[arg(long)]
        grain: Option<String>,

        /// SQL dialect
        #[arg(long, default_value = "duckdb")]
        dialect: DialectArg,

        /// Row limit
        #[arg(long)]
        limit: Option<u64>,
    },
}

#[derive(Clone, ValueEnum)]
enum DialectArg {
    Duckdb,
    Bigquery,
    Snowflake,
    Redshift,
    Postgres,
    Databricks,
    Trino,
}

impl From<DialectArg> for SqlDialect {
    fn from(d: DialectArg) -> Self {
        match d {
            DialectArg::Duckdb => SqlDialect::DuckDB,
            DialectArg::Bigquery => SqlDialect::BigQuery,
            DialectArg::Snowflake => SqlDialect::Snowflake,
            DialectArg::Redshift => SqlDialect::Redshift,
            DialectArg::Postgres => SqlDialect::Postgres,
            DialectArg::Databricks => SqlDialect::Databricks,
            DialectArg::Trino => SqlDialect::Trino,
        }
    }
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Query {
            manifest,
            metrics,
            group_by,
            grain,
            dialect,
            limit,
        } => {
            let manifest_json = std::fs::read_to_string(&manifest).unwrap_or_else(|e| {
                eprintln!("Error reading manifest {}: {e}", manifest.display());
                std::process::exit(1);
            });

            let manifest: mf_core::manifest::SemanticManifest =
                serde_json::from_str(&manifest_json).unwrap_or_else(|e| {
                    eprintln!("Error parsing manifest: {e}");
                    std::process::exit(1);
                });

            // Parse group-by specs following Python MetricFlow's dundered name convention.
            // Split on "__" and check if the last part is a time granularity.
            // Examples:
            //   "metric_time" → TimeDimension { name: "metric_time", grain }
            //   "ds__month" → TimeDimension { name: "ds", grain: Month }
            //   "customer_arr_waterfall__close_month" → TimeDimension { name: "close", grain: Month, entity: ["customer_arr_waterfall"] }
            //     BUT "close_month" is not a grain → Dimension { name: "close_month", entity: ["customer_arr_waterfall"] }
            //   "listing__country" → Dimension { name: "country", entity: ["listing"] }
            let group_by_specs: Vec<GroupBySpec> = group_by
                .iter()
                .map(|g| parse_group_by_spec(g, grain.as_deref()))
                .collect();

            let query = QuerySpec {
                metrics,
                group_by: group_by_specs,
                where_clauses: vec![],
                order_by: vec![],
                limit,
            };

            match mf_sql::compile_query(&manifest, &query, dialect.into()) {
                Ok(sql) => println!("{sql}"),
                Err(e) => {
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
}

/// Parse a dundered group-by name following Python MetricFlow's convention.
///
/// Split on `__` and check if the last part is a time granularity:
/// - `"metric_time"` with --grain day → TimeDimension { name: "metric_time", grain: Day }
/// - `"ds__month"` → TimeDimension { name: "ds", grain: Month }
/// - `"listing__country"` → Dimension { name: "country", entity_path: ["listing"] }
/// - `"listing__ds__week"` → TimeDimension { name: "ds", grain: Week, entity_path: ["listing"] }
fn parse_group_by_spec(input: &str, cli_grain: Option<&str>) -> GroupBySpec {
    let parts: Vec<&str> = input.split("__").collect();

    // Single part, no dunder
    if parts.len() == 1 {
        let name = parts[0];
        // If --grain provided and name looks like a time dimension, treat as TimeDimension
        if let Some(gr) = cli_grain {
            if name == "metric_time" || name.contains("time") || name.contains("date") {
                return GroupBySpec::TimeDimension {
                    name: name.to_string(),
                    grain: gr.parse().unwrap_or(TimeGrain::Day),
                    entity_path: vec![],
                };
            }
        }
        return GroupBySpec::Dimension {
            name: name.to_string(),
            entity_path: vec![],
        };
    }

    // Check if the last part is a time granularity
    let last = parts[parts.len() - 1];
    if let Ok(grain) = last.parse::<TimeGrain>() {
        // Last part is a grain. Element name is second-to-last, entity path is everything before.
        if parts.len() == 2 {
            // e.g. "ds__month" → name="ds", grain=Month, no entity
            return GroupBySpec::TimeDimension {
                name: parts[0].to_string(),
                grain,
                entity_path: vec![],
            };
        }
        // e.g. "listing__ds__week" → entity=["listing"], name="ds", grain=Week
        let entity_path: Vec<String> = parts[..parts.len() - 2].iter().map(|s| s.to_string()).collect();
        return GroupBySpec::TimeDimension {
            name: parts[parts.len() - 2].to_string(),
            grain,
            entity_path,
        };
    }

    // Last part is NOT a grain → it's a dimension name, everything before is entity path.
    // e.g. "listing__country" → entity=["listing"], name="country"
    // e.g. "customer_arr_waterfall__close_month" → entity=["customer_arr_waterfall"], name="close_month"
    let entity_path: Vec<String> = parts[..parts.len() - 1].iter().map(|s| s.to_string()).collect();
    let name = parts[parts.len() - 1].to_string();

    // If --grain is provided, check if this dimension should be treated as a time dimension
    if let Some(gr) = cli_grain {
        return GroupBySpec::TimeDimension {
            name,
            grain: gr.parse().unwrap_or(TimeGrain::Day),
            entity_path,
        };
    }

    GroupBySpec::Dimension {
        name,
        entity_path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_dimension() {
        let spec = parse_group_by_spec("is_instant", None);
        assert!(matches!(spec, GroupBySpec::Dimension { ref name, ref entity_path } if name == "is_instant" && entity_path.is_empty()));
    }

    #[test]
    fn test_parse_metric_time_with_grain_flag() {
        let spec = parse_group_by_spec("metric_time", Some("day"));
        assert!(matches!(spec, GroupBySpec::TimeDimension { ref name, grain, ref entity_path }
            if name == "metric_time" && grain == TimeGrain::Day && entity_path.is_empty()));
    }

    #[test]
    fn test_parse_metric_time_with_dunder_grain() {
        // "metric_time__day" → TimeDimension, grain from dunder
        let spec = parse_group_by_spec("metric_time__day", None);
        assert!(matches!(spec, GroupBySpec::TimeDimension { ref name, grain, ref entity_path }
            if name == "metric_time" && grain == TimeGrain::Day && entity_path.is_empty()));
    }

    #[test]
    fn test_parse_entity_dunder_dimension() {
        // "listing__country" → Dimension with entity path
        let spec = parse_group_by_spec("listing__country", None);
        match spec {
            GroupBySpec::Dimension { name, entity_path } => {
                assert_eq!(name, "country");
                assert_eq!(entity_path, vec!["listing"]);
            }
            other => panic!("expected Dimension, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_entity_dunder_dimension_not_a_grain() {
        // "customer_arr_waterfall__close_month" — "close_month" is NOT a grain
        let spec = parse_group_by_spec("customer_arr_waterfall__close_month", None);
        match spec {
            GroupBySpec::Dimension { name, entity_path } => {
                assert_eq!(name, "close_month");
                assert_eq!(entity_path, vec!["customer_arr_waterfall"]);
            }
            other => panic!("expected Dimension, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_entity_dunder_time_dimension_with_grain() {
        // "listing__ds__week" → TimeDimension with entity path and grain
        let spec = parse_group_by_spec("listing__ds__week", None);
        match spec {
            GroupBySpec::TimeDimension { name, grain, entity_path } => {
                assert_eq!(name, "ds");
                assert_eq!(grain, TimeGrain::Week);
                assert_eq!(entity_path, vec!["listing"]);
            }
            other => panic!("expected TimeDimension, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_ds_dunder_month() {
        // "ds__month" → TimeDimension, no entity path
        let spec = parse_group_by_spec("ds__month", None);
        match spec {
            GroupBySpec::TimeDimension { name, grain, entity_path } => {
                assert_eq!(name, "ds");
                assert_eq!(grain, TimeGrain::Month);
                assert!(entity_path.is_empty());
            }
            other => panic!("expected TimeDimension, got {other:?}"),
        }
    }
}
