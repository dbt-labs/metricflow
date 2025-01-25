test_name: test_metric_time_filter_with_two_targets
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimization for a simple metric time predicate through a single join.

      This is currently a no-op for the pushdown optimizer.
      TODO: support metric time pushdown
sql_engine: Databricks
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'listing__country_latest']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  listing__country_latest
  , SUM(bookings) AS bookings
FROM (
  -- Join Standard Outputs
  SELECT
    listings_latest_src_28000.country AS listing__country_latest
    , nr_subq_7.metric_time__day AS metric_time__day
    , nr_subq_7.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_7
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    nr_subq_7.listing = listings_latest_src_28000.listing_id
) nr_subq_10
WHERE metric_time__day = '2024-01-01'
GROUP BY
  listing__country_latest
