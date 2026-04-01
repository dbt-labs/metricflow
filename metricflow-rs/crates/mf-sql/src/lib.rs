pub mod ast;
pub mod convert;
pub mod duckdb;
pub mod render;

use mf_core::dialect::SqlDialect;
use mf_core::manifest::SemanticManifest;
use mf_core::spec::QuerySpec;
use mf_manifest::graph::SemanticGraph;
use mf_planning::builder;

/// Top-level API: compile a query to SQL.
pub fn compile_query(
    manifest: &SemanticManifest,
    query: &QuerySpec,
    dialect: SqlDialect,
) -> Result<String, Box<dyn std::error::Error>> {
    let graph = SemanticGraph::build(manifest)?;
    let plan = builder::build_plan(&graph, query)?;
    let sql_node = convert::to_sql(&plan, &graph)?;
    let renderer = render::renderer_for_dialect(dialect);
    Ok(renderer.render(&sql_node))
}
