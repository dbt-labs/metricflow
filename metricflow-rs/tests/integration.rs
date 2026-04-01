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

// ─── Derived metric tests (Phase 4) ──────────────────────────────────────────

#[test]
fn test_end_to_end_derived_metric() {
    let manifest_json = include_str!("fixtures/derived_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings_growth".into()],
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

    eprintln!("Generated SQL (derived):\n{sql}");

    assert!(
        sql.contains("FULL OUTER JOIN"),
        "should have FULL OUTER JOIN: {sql}"
    );
    assert!(
        sql.contains("bookings - instant_bookings"),
        "should contain derived expression: {sql}"
    );
    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
    assert!(
        sql.contains("metric_time__day"),
        "should have time dimension: {sql}"
    );
}

#[test]
fn test_end_to_end_ratio_metric() {
    let manifest_json = include_str!("fixtures/ratio_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["instant_booking_rate".into()],
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

    eprintln!("Generated SQL (ratio):\n{sql}");

    assert!(
        sql.contains("FULL OUTER JOIN"),
        "should have FULL OUTER JOIN: {sql}"
    );
    assert!(sql.contains("NULLIF"), "should have NULLIF: {sql}");
    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
}

// ─── Cumulative metric tests (Phase 4, Tasks 8-10) ───────────────────────────

#[test]
fn test_end_to_end_trailing_7d_bookings() {
    let manifest_json = include_str!("fixtures/cumulative_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["trailing_7d_bookings".into()],
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

    eprintln!("Generated SQL (trailing_7d_bookings):\n{sql}");

    assert!(
        sql.contains("demo.mf_time_spine"),
        "should reference time spine table: {sql}"
    );
    assert!(
        sql.contains("INTERVAL"),
        "should have INTERVAL for trailing window: {sql}"
    );
    assert!(sql.contains("7"), "should have window count of 7: {sql}");
    assert!(
        sql.contains("INNER JOIN"),
        "should have INNER JOIN to source: {sql}"
    );
    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
    assert!(
        sql.contains("metric_time__day"),
        "should have metric_time dimension: {sql}"
    );
    assert!(sql.contains("GROUP BY"), "should have GROUP BY: {sql}");
}

#[test]
fn test_end_to_end_bookings_mtd() {
    let manifest_json = include_str!("fixtures/cumulative_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings_mtd".into()],
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

    eprintln!("Generated SQL (bookings_mtd):\n{sql}");

    assert!(
        sql.contains("demo.mf_time_spine"),
        "should reference time spine table: {sql}"
    );
    assert!(
        sql.contains("DATE_TRUNC"),
        "should have DATE_TRUNC for grain_to_date: {sql}"
    );
    assert!(sql.contains("month"), "should reference month grain: {sql}");
    assert!(
        sql.contains("INNER JOIN"),
        "should have INNER JOIN to source: {sql}"
    );
    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
    assert!(
        sql.contains("metric_time__day"),
        "should have metric_time dimension: {sql}"
    );
}

#[test]
fn test_end_to_end_bookings_all_time() {
    let manifest_json = include_str!("fixtures/cumulative_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings_all_time".into()],
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

    eprintln!("Generated SQL (bookings_all_time):\n{sql}");

    assert!(
        sql.contains("demo.mf_time_spine"),
        "should reference time spine table: {sql}"
    );
    // All-time has no INTERVAL or DATE_TRUNC
    assert!(
        !sql.contains("INTERVAL"),
        "all-time should not have INTERVAL: {sql}"
    );
    assert!(
        !sql.contains("DATE_TRUNC"),
        "all-time should not have DATE_TRUNC: {sql}"
    );
    assert!(
        sql.contains("INNER JOIN"),
        "should have INNER JOIN to source: {sql}"
    );
    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
}

// ─── Real manifest format tests (metric_aggregation_params) ─────────────────

#[test]
fn test_end_to_end_real_format_simple_metric() {
    let manifest_json = include_str!("fixtures/real_format_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["arr_current".into()],
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

    eprintln!("Generated SQL (real format simple):\n{sql}");

    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
    assert!(
        sql.contains("month_ending_current_arr"),
        "should use expr from metric_aggregation_params: {sql}"
    );
    assert!(
        sql.contains("DATE_TRUNC('day', date_month)"),
        "should DATE_TRUNC the actual time column (date_month), not a logical name: {sql}"
    );
    assert!(
        sql.contains("metric_time__day"),
        "should alias as metric_time__day: {sql}"
    );
    assert!(
        sql.contains("fct_customer_arr_waterfall"),
        "should reference the table: {sql}"
    );
    // Table alias should NOT have dots
    assert!(
        !sql.contains("analytics.dbt_demo.fct_customer_arr_waterfall_src"),
        "table alias should not contain dots: {sql}"
    );
    assert!(sql.contains("GROUP BY"), "should have GROUP BY: {sql}");
}

#[test]
fn test_end_to_end_real_format_simple_metric_no_groupby() {
    let manifest_json = include_str!("fixtures/real_format_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["arr_current".into()],
        group_by: vec![],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();

    eprintln!("Generated SQL (real format no group-by):\n{sql}");

    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
    assert!(
        sql.contains("month_ending_current_arr"),
        "should use expr from metric_aggregation_params: {sql}"
    );
    assert!(
        !sql.contains("GROUP BY"),
        "should not have GROUP BY: {sql}"
    );
}

#[test]
fn test_end_to_end_real_format_derived_metric() {
    let manifest_json = include_str!("fixtures/real_format_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["arr_growth".into()],
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

    eprintln!("Generated SQL (real format derived):\n{sql}");

    assert!(
        sql.contains("FULL OUTER JOIN"),
        "should have FULL OUTER JOIN: {sql}"
    );
    assert!(
        sql.contains("arr_current - arr_new"),
        "should contain derived expression: {sql}"
    );
    // Both input metrics should resolve through metric_aggregation_params
    assert!(
        sql.contains("month_ending_current_arr"),
        "should use arr_current's expr: {sql}"
    );
    assert!(
        sql.contains("new_arr_amount"),
        "should use arr_new's expr: {sql}"
    );
}

#[test]
fn test_end_to_end_real_format_cumulative_metric() {
    let manifest_json = include_str!("fixtures/real_format_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["trailing_30d_arr".into()],
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

    eprintln!("Generated SQL (real format cumulative):\n{sql}");

    assert!(
        sql.contains("dbt_demo.mf_time_spine"),
        "should reference time spine table: {sql}"
    );
    assert!(
        sql.contains("INTERVAL"),
        "should have INTERVAL for trailing window: {sql}"
    );
    assert!(sql.contains("30"), "should have window count of 30: {sql}");
    assert!(
        sql.contains("INNER JOIN"),
        "should have INNER JOIN to source: {sql}"
    );
    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
    // The underlying measure should come from the input metric's metric_aggregation_params
    assert!(
        sql.contains("month_ending_current_arr"),
        "should use the underlying measure expr: {sql}"
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
