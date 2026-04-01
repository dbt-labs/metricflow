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

// ─── Metric-level filter tests ───────────────────────────────────────────────

#[test]
fn test_end_to_end_real_format_metric_with_filter() {
    let manifest_json = include_str!("fixtures/real_format_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["arr_current_enterprise".into()],
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

    eprintln!("Generated SQL (filtered metric):\n{sql}");

    assert!(sql.contains("SUM"), "should have SUM aggregation: {sql}");
    assert!(
        sql.contains("month_ending_current_arr"),
        "should use expr from metric_aggregation_params: {sql}"
    );
    // The metric-level filter should produce a WHERE clause
    assert!(
        sql.contains("WHERE"),
        "should have WHERE clause from metric filter: {sql}"
    );
    assert!(
        sql.contains("Enterprise"),
        "should filter on Enterprise: {sql}"
    );
    assert!(
        sql.contains("customer__plan_type"),
        "should reference the resolved filter column: {sql}"
    );
}

// ─── Multi-metric query tests ────────────────────────────────────────────────

#[test]
fn test_end_to_end_multi_metric_two_simple() {
    let manifest_json = include_str!("fixtures/derived_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings".into(), "instant_bookings".into()],
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

    eprintln!("Generated SQL (multi-metric):\n{sql}");

    // Should use FULL OUTER JOIN to combine the two metrics
    assert!(
        sql.contains("FULL OUTER JOIN"),
        "multi-metric should use FULL OUTER JOIN: {sql}"
    );
    // Should COALESCE the shared group-by dimension
    assert!(
        sql.contains("COALESCE"),
        "should COALESCE group-by columns: {sql}"
    );
    // Both metrics should appear
    assert!(
        sql.contains("bookings"),
        "should contain bookings metric: {sql}"
    );
    assert!(
        sql.contains("instant_bookings"),
        "should contain instant_bookings metric: {sql}"
    );
    // Each metric should have its own aggregation subquery
    assert!(
        sql.contains("SUM"),
        "should have SUM aggregation: {sql}"
    );
    assert!(
        sql.contains("GROUP BY"),
        "should have GROUP BY: {sql}"
    );
}

#[test]
fn test_end_to_end_multi_metric_no_groupby() {
    let manifest_json = include_str!("fixtures/derived_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings".into(), "instant_bookings".into()],
        group_by: vec![],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();

    eprintln!("Generated SQL (multi-metric no group-by):\n{sql}");

    // Without group-by, should use CROSS JOIN
    assert!(
        sql.contains("CROSS JOIN"),
        "multi-metric without group-by should use CROSS JOIN: {sql}"
    );
    assert!(
        sql.contains("bookings"),
        "should contain bookings: {sql}"
    );
    assert!(
        sql.contains("instant_bookings"),
        "should contain instant_bookings: {sql}"
    );
}

#[test]
fn test_end_to_end_multi_metric_with_simple_and_derived() {
    let manifest_json = include_str!("fixtures/derived_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    // Mix simple + derived in one query
    let query = QuerySpec {
        metrics: vec!["bookings".into(), "bookings_growth".into()],
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

    eprintln!("Generated SQL (simple + derived multi-metric):\n{sql}");

    // Should have FULL OUTER JOIN at the top level to combine the two metrics
    assert!(
        sql.contains("FULL OUTER JOIN"),
        "should use FULL OUTER JOIN: {sql}"
    );
    // The derived metric expression should appear
    assert!(
        sql.contains("bookings - instant_bookings"),
        "should contain derived expression: {sql}"
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

#[test]
fn test_end_to_end_query_level_where_filter() {
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
        where_clauses: vec![
            "{{ Dimension('customer__plan_type') }} = 'Enterprise'".into(),
        ],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();
    let sql_upper = sql.to_uppercase();

    // The WHERE clause should appear in the SQL
    assert!(
        sql.contains("customer__plan_type = 'Enterprise'"),
        "SQL should contain resolved WHERE filter: {sql}"
    );
    // The filter column should be projected
    assert!(
        sql_upper.contains("PLAN_TYPE"),
        "SQL should reference plan_type column: {sql}"
    );
}

// ─── FULL OUTER JOIN NULL deduplication tests ───────────────────────────────

#[test]
fn test_combine_aggregated_outputs_reaggregates_for_null_dedup() {
    // When combining 2+ aggregated subqueries via FULL OUTER JOIN with group-by columns,
    // the result must be wrapped in a GROUP BY + MAX to handle NULL = NULL not matching.
    let manifest_json = include_str!("fixtures/derived_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings".into(), "instant_bookings".into()],
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

    eprintln!("Generated SQL (combine reaggregation):\n{sql}");

    // The FULL OUTER JOIN must be wrapped in a re-aggregation GROUP BY
    // to handle NULL dimension values (NULL = NULL returns NULL in SQL).
    assert!(
        sql.contains("MAX("),
        "should wrap metric columns in MAX() for NULL dedup: {sql}"
    );
    // The outer query should GROUP BY the coalesced dimension column
    let combine_idx = sql.find("combine_subq_").expect("should have combine_subq_ alias");
    let after_combine = &sql[combine_idx..];
    assert!(
        after_combine.contains("GROUP BY"),
        "should GROUP BY the coalesced columns after FULL OUTER JOIN: {sql}"
    );
}

#[test]
fn test_combine_aggregated_outputs_no_reaggregation_without_groupby() {
    // When there's no group-by (CROSS JOIN), no re-aggregation is needed.
    let manifest_json = include_str!("fixtures/derived_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings".into(), "instant_bookings".into()],
        group_by: vec![],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let sql = mf_sql::compile_query(&manifest, &query, SqlDialect::DuckDB).unwrap();

    eprintln!("Generated SQL (combine no group-by):\n{sql}");

    // CROSS JOIN, no re-aggregation needed
    assert!(
        sql.contains("CROSS JOIN"),
        "should use CROSS JOIN without group-by: {sql}"
    );
    assert!(
        !sql.contains("MAX("),
        "should NOT wrap in MAX() without group-by: {sql}"
    );
}

#[test]
fn test_three_way_full_outer_join_uses_coalesce_in_on_clause() {
    // When 3+ aggregated subqueries are combined, the ON clause for the 3rd+ join
    // should use COALESCE of previously-seen aliases, not just the first alias.
    let manifest_json = include_str!("fixtures/filter_pushdown_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    // nps_score is a derived metric with 3 inputs → 3-way FULL OUTER JOIN
    let query = QuerySpec {
        metrics: vec!["nps_score".into()],
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

    eprintln!("Generated SQL (3-way FULL OUTER JOIN):\n{sql}");

    // Count FULL OUTER JOINs — should be at least 2 (for 3 subqueries)
    let foj_count = sql.matches("FULL OUTER JOIN").count();
    assert!(
        foj_count >= 2,
        "should have at least 2 FULL OUTER JOINs for 3 inputs, got {foj_count}: {sql}"
    );

    // The second FULL OUTER JOIN's ON clause should use COALESCE of the first two aliases.
    // Pattern: COALESCE(agg_subq_N.col, agg_subq_M.col) = agg_subq_K.col
    // Find the second FULL OUTER JOIN and check its ON clause has COALESCE.
    let second_foj = sql
        .match_indices("FULL OUTER JOIN")
        .nth(1)
        .expect("should have 2nd FULL OUTER JOIN");
    let after_second_foj = &sql[second_foj.0..];
    let on_pos = after_second_foj
        .find("\n")
        .and_then(|_| after_second_foj.find("ON"))
        .expect("should have ON after 2nd FULL OUTER JOIN");
    let on_clause = &after_second_foj[on_pos..on_pos + 200.min(after_second_foj.len() - on_pos)];
    assert!(
        on_clause.contains("COALESCE("),
        "second FULL OUTER JOIN ON clause should use COALESCE: {on_clause}"
    );
}

// ─── Derived metric filter pushdown tests ───────────────────────────────────

#[test]
fn test_derived_metric_filter_pushdown_to_child_metrics() {
    // nps_enterprise has a filter (plan_tier = 'enterprise') that must be pushed
    // down to all 3 child simple metrics (respondents, promoters, detractors).
    let manifest_json = include_str!("fixtures/filter_pushdown_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["nps_enterprise".into()],
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

    eprintln!("Generated SQL (filter pushdown):\n{sql}");

    // The derived metric filter should appear in each child metric's WHERE clause.
    // Count occurrences of the filter — should be 3 (once per child).
    let filter_count = sql.matches("plan_tier").count();
    assert!(
        filter_count >= 3,
        "derived metric filter should be pushed to all 3 child metrics, found {filter_count} occurrences: {sql}"
    );
    assert!(
        sql.contains("'enterprise'"),
        "should filter on enterprise: {sql}"
    );
}

#[test]
fn test_different_derived_metrics_produce_different_filters() {
    // nps_enterprise filters on 'enterprise', nps_developer filters on 'developer'.
    // They must produce different SQL.
    let manifest_json = include_str!("fixtures/filter_pushdown_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let enterprise_query = QuerySpec {
        metrics: vec!["nps_enterprise".into()],
        group_by: vec![GroupBySpec::TimeDimension {
            name: "metric_time".into(),
            grain: TimeGrain::Day,
            entity_path: vec![],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let developer_query = QuerySpec {
        metrics: vec!["nps_developer".into()],
        group_by: vec![GroupBySpec::TimeDimension {
            name: "metric_time".into(),
            grain: TimeGrain::Day,
            entity_path: vec![],
        }],
        where_clauses: vec![],
        order_by: vec![],
        limit: None,
    };

    let enterprise_sql =
        mf_sql::compile_query(&manifest, &enterprise_query, SqlDialect::DuckDB).unwrap();
    let developer_sql =
        mf_sql::compile_query(&manifest, &developer_query, SqlDialect::DuckDB).unwrap();

    eprintln!("Enterprise SQL:\n{enterprise_sql}");
    eprintln!("Developer SQL:\n{developer_sql}");

    // Enterprise SQL should contain 'enterprise' but not 'developer'
    assert!(
        enterprise_sql.contains("'enterprise'"),
        "enterprise SQL should filter on enterprise: {enterprise_sql}"
    );
    assert!(
        !enterprise_sql.contains("'developer'"),
        "enterprise SQL should NOT contain developer filter: {enterprise_sql}"
    );

    // Developer SQL should contain 'developer' but not 'enterprise'
    assert!(
        developer_sql.contains("'developer'"),
        "developer SQL should filter on developer: {developer_sql}"
    );
    assert!(
        !developer_sql.contains("'enterprise'"),
        "developer SQL should NOT contain enterprise filter: {developer_sql}"
    );
}

#[test]
fn test_derived_metric_without_filter_has_no_extra_where() {
    // nps_score has no filter — only the child metrics' own filters should appear.
    let manifest_json = include_str!("fixtures/filter_pushdown_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["nps_score".into()],
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

    eprintln!("Generated SQL (no pushdown):\n{sql}");

    // plan_tier should NOT appear (no derived-level filter)
    assert!(
        !sql.contains("plan_tier"),
        "unfiltered nps_score should not have plan_tier filter: {sql}"
    );
    // But child metric filters (nps_category) should still be present
    assert!(
        sql.contains("nps_category"),
        "child metric filters should still be present: {sql}"
    );
}

// ─── Per-input-metric filter tests ──────────────────────────────────────────

#[test]
fn test_per_input_metric_filters() {
    // nps_with_input_filters has per-input filters (survey_type = 'cloud')
    // on each input metric.
    let manifest_json = include_str!("fixtures/filter_pushdown_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["nps_with_input_filters".into()],
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

    eprintln!("Generated SQL (per-input filters):\n{sql}");

    // The per-input filter (survey_type = 'cloud') should appear for each input.
    let cloud_count = sql.matches("'cloud'").count();
    assert!(
        cloud_count >= 3,
        "per-input filter should appear for all 3 inputs, found {cloud_count} occurrences: {sql}"
    );
    // The child metric filters (nps_category) should also be present for promoters/detractors
    let nps_cat_count = sql.matches("nps_category").count();
    assert!(
        nps_cat_count >= 2,
        "child metric nps_category filters should be present, found {nps_cat_count}: {sql}"
    );
}

#[test]
fn test_derived_metric_filter_combined_with_child_metric_filter() {
    // nps_enterprise pushes plan_tier='enterprise' to child metrics.
    // The 'promoters' child already has its own filter (nps_category='promoter').
    // Both filters should appear together in the promoters subquery.
    let manifest_json = include_str!("fixtures/filter_pushdown_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["nps_enterprise".into()],
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

    eprintln!("Generated SQL (combined filters):\n{sql}");

    // Find WHERE clauses that contain BOTH the derived-level filter AND child filter.
    // The promoters subquery should have: nps_category = 'promoter' AND plan_tier = 'enterprise'
    // Split on WHERE to find individual filter blocks.
    let where_blocks: Vec<&str> = sql.split("WHERE").collect();
    let combined_filter_blocks: Vec<&&str> = where_blocks
        .iter()
        .filter(|block| block.contains("'promoter'") && block.contains("'enterprise'"))
        .collect();
    assert!(
        !combined_filter_blocks.is_empty(),
        "promoters subquery should have BOTH nps_category='promoter' AND plan_tier='enterprise': {sql}"
    );

    let detractor_blocks: Vec<&&str> = where_blocks
        .iter()
        .filter(|block| block.contains("'detractor'") && block.contains("'enterprise'"))
        .collect();
    assert!(
        !detractor_blocks.is_empty(),
        "detractors subquery should have BOTH nps_category='detractor' AND plan_tier='enterprise': {sql}"
    );
}

// ─── fill_nulls_with in FULL OUTER JOIN re-aggregation ──────────────────────

#[test]
fn test_fill_nulls_with_in_combine_reaggregation() {
    // When child metrics have fill_nulls_with=0, the re-aggregation after
    // FULL OUTER JOIN should use COALESCE(MAX(col), 0) instead of just MAX(col).
    // This handles the case where a dimension value has no matching rows in one
    // branch (e.g., no detractors for is_reseller=true → detractors should be 0, not NULL).
    let manifest_json = include_str!("fixtures/filter_pushdown_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["nps_score".into()],
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

    eprintln!("Generated SQL (fill_nulls_with in combine):\n{sql}");

    // All three metric columns should be wrapped in COALESCE(MAX(...), 0)
    let coalesce_max_count = sql.matches("COALESCE(MAX(").count();
    assert!(
        coalesce_max_count >= 3,
        "all 3 metric columns should use COALESCE(MAX(...), 0), found {coalesce_max_count}: {sql}"
    );
    // Verify the fill value is 0
    assert!(
        sql.contains("COALESCE(MAX(") && sql.contains("), 0)"),
        "should use fill value 0: {sql}"
    );
}

#[test]
fn test_no_fill_nulls_with_uses_plain_max() {
    // When metrics do NOT have fill_nulls_with, the re-aggregation uses plain MAX().
    let manifest_json = include_str!("fixtures/derived_manifest.json");
    let manifest: mf_core::manifest::SemanticManifest =
        serde_json::from_str(manifest_json).unwrap();

    let query = QuerySpec {
        metrics: vec!["bookings".into(), "instant_bookings".into()],
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

    eprintln!("Generated SQL (no fill_nulls_with):\n{sql}");

    // Should have MAX but NOT COALESCE(MAX(...))
    assert!(
        sql.contains("MAX("),
        "should use MAX in re-aggregation: {sql}"
    );
    assert!(
        !sql.contains("COALESCE(MAX("),
        "should NOT use COALESCE(MAX(...)) without fill_nulls_with: {sql}"
    );
}
