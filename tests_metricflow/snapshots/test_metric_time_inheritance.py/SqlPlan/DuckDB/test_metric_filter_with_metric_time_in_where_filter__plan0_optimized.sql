test_name: test_metric_filter_with_metric_time_in_where_filter
test_filename: test_metric_time_inheritance.py
docstring:
  Tests a query with a metric filter in the where clause that includes metric_time in group_by.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listings', 'metric_time__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  SELECT
    subq_22.listing__bookings AS listing__bookings
    , subq_16.metric_time__month AS metric_time__month
    , subq_16.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('month', created_at) AS metric_time__month
      , listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_16
  LEFT OUTER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookings']
    SELECT
      listing
      , SUM(bookings) AS listing__bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'listing']
      SELECT
        listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_19
    GROUP BY
      listing
  ) subq_22
  ON
    subq_16.listing = subq_22.listing
) subq_23
WHERE listing__bookings > 0
GROUP BY
  metric_time__month
