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

            // Parse group-by specs (simplified: if grain is provided and name contains "time", treat as time dimension)
            let group_by_specs: Vec<GroupBySpec> = group_by
                .iter()
                .map(|g| {
                    if let Some(ref gr) = grain {
                        if g.contains("metric_time") || g.contains("time") {
                            return GroupBySpec::TimeDimension {
                                name: g.clone(),
                                grain: gr.parse().unwrap_or(TimeGrain::Day),
                                entity_path: vec![],
                            };
                        }
                    }
                    GroupBySpec::Dimension {
                        name: g.clone(),
                        entity_path: vec![],
                    }
                })
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
