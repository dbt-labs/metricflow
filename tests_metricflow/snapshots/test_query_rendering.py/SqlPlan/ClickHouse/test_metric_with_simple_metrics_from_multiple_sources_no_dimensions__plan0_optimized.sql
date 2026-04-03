test_name: test_metric_with_simple_metrics_from_multiple_sources_no_dimensions
test_filename: test_query_rendering.py
sql_engine: ClickHouse
---
SELECT
  CAST(MAX(subq_19.bookings) AS Nullable(Float64)) / CAST(NULLIF(MAX(subq_25.listings), 0) AS Nullable(Float64)) AS bookings_per_listing
FROM (
  SELECT
    SUM(1) AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_19
CROSS JOIN (
  SELECT
    SUM(1) AS listings
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_25
