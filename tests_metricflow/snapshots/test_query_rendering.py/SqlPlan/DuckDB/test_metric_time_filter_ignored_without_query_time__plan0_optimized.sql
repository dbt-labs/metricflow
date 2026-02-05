test_name: test_metric_time_filter_ignored_without_query_time
test_filename: test_query_rendering.py
docstring:
  Tests metric_time in a metric filter when the query has no time dimension.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__active_listings_with_metric_time']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  SUM(active_listings_with_metric_time) AS active_listings_with_metric_time
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__active_listings_with_metric_time', 'listing__bookings']
  SELECT
    subq_27.listing__bookings AS listing__bookings
    , subq_20.__active_listings_with_metric_time AS active_listings_with_metric_time
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , 1 AS __active_listings_with_metric_time
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_20
  LEFT OUTER JOIN (
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookings']
    SELECT
      listing
      , SUM(__bookings) AS listing__bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['__bookings', 'listing']
      -- Pass Only Elements: ['__bookings', 'listing']
      SELECT
        listing_id AS listing
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_24
    GROUP BY
      listing
  ) subq_27
  ON
    subq_20.listing = subq_27.listing
) subq_29
WHERE listing__bookings > 2
