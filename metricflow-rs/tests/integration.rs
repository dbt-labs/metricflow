use mf_core::dialect::SqlDialect;
use mf_core::spec::*;
use mf_core::types::*;

#[test]
fn test_end_to_end_simple_metric_duckdb() {
    let manifest_json = include_str!("fixtures/simple_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings".into()],
        group_by: vec![GroupBySpec::TimeDimension {
            name: "metric_time".into(),
            grain: TimeGrain::Day,
            entity_path: vec![],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();

    // The SQL should contain key elements
    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
    assert!(
        sql.contains("__bookings"),
        "should have internal measure alias: {sql}"
    );
    assert!(
        sql.contains("bookings") && sql.contains("AS"),
        "should have final alias: {sql}"
    );
    assert!(
        sql.contains("metric_time__day"),
        "should have time dimension in GROUP BY: {sql}"
    );
    assert!(
        sql.contains("demo.fct_bookings"),
        "should reference source table: {sql}"
    );
    assert!(sql.contains("GROUP BY"), "should have GROUP BY: {sql}");

    // Print for manual inspection
    eprintln!("Generated SQL:\n{sql}");
}

#[test]
fn test_end_to_end_with_categorical_dimension() {
    let manifest_json = include_str!("fixtures/simple_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings".into()],
        group_by: vec![
            GroupBySpec::TimeDimension {
                name: "metric_time".into(),
                grain: TimeGrain::Day,
                entity_path: vec![],
            },
            GroupBySpec::Dimension {
                name: "is_instant".into(),
                entity_path: vec![],
            },
        ],
        where_clauses: vec![],
        order_by: vec![],
        limit: Some(10),
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();

    assert!(
        sql.contains("is_instant"),
        "should include dimension: {sql}"
    );
    assert!(sql.contains("LIMIT 10"), "should have LIMIT: {sql}");

    eprintln!("Generated SQL:\n{sql}");
}

// ─── Two-model manifest tests (Task 6) ────────────────────────────────────────

#[test]
fn test_end_to_end_join_dimension_generates_left_outer_join() {
    let manifest_json = include_str!("fixtures/two_model_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    // Query: bookings metric grouped by listing__country — requires a join to listings_source
    let query = QuerySpec {
        metrics: vec!["bookings".into()],
        group_by: vec![GroupBySpec::Dimension {
            name: "country".into(),
            entity_path: vec!["listing".into()],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();

    eprintln!("Generated SQL (join):\n{sql}");

    assert!(
        sql.contains("LEFT OUTER JOIN"),
        "should have LEFT OUTER JOIN: {sql}"
    );
    assert!(
        sql.contains("demo.fct_bookings"),
        "should reference bookings table: {sql}"
    );
    assert!(
        sql.contains("demo.dim_listings_latest"),
        "should reference listings table: {sql}"
    );
    assert!(
        sql.contains("listing__country"),
        "should have prefixed dimension name: {sql}"
    );
    assert!(sql.contains("listing_id"), "should include join key: {sql}");
    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
    assert!(sql.contains("GROUP BY"), "should have GROUP BY: {sql}");
    assert!(
        sql.contains("__bookings"),
        "should have internal measure alias: {sql}"
    );
}

#[test]
fn test_end_to_end_join_dimension_projects_right_side_columns() {
    let manifest_json = include_str!("fixtures/two_model_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    // Query: bookings metric grouped by listing__is_lux_listing — another dimension from listings_source
    let query = QuerySpec {
        metrics: vec!["bookings".into()],
        group_by: vec![GroupBySpec::Dimension {
            name: "is_lux_listing".into(),
            entity_path: vec!["listing".into()],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();

    eprintln!("Generated SQL (join is_lux_listing):\n{sql}");

    assert!(
        sql.contains("LEFT OUTER JOIN"),
        "should have LEFT OUTER JOIN: {sql}"
    );
    assert!(
        sql.contains("listing__is_lux_listing"),
        "should have prefixed dimension name: {sql}"
    );
    // The physical column is_lux_listing should be projected from the right table
    assert!(
        sql.contains("is_lux_listing"),
        "should contain is_lux_listing column: {sql}"
    );
    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
    assert!(sql.contains("GROUP BY"), "should have GROUP BY: {sql}");
    // ON clause should reference listing_id on both sides
    assert!(
        sql.contains("listing_id"),
        "should include join key in ON clause: {sql}"
    );
}

// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn test_end_to_end_unknown_metric_error() {
    let manifest_json = include_str!("fixtures/simple_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["nonexistent".into()],
        group_by: vec![],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let result = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("nonexistent"),
        "error should mention metric name: {err}"
    );
}
