test_name: test_metric_time_filter_with_two_targets
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimization for a simple metric time predicate through a single join.

      This is currently a no-op for the pushdown optimizer.
      TODO: support metric time pushdown
sql_engine: Redshift
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__bookings', 'listing__country_latest']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  listing__country_latest
  , SUM(bookings) AS bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__bookings', 'listing__country_latest', 'metric_time__day']
  SELECT
    subq_12.metric_time__day AS metric_time__day
    , listings_latest_src_28000.country AS listing__country_latest
    , subq_12.__bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS __bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_12
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_12.listing = listings_latest_src_28000.listing_id
) subq_17
WHERE metric_time__day = '2024-01-01'
GROUP BY
  listing__country_latest
