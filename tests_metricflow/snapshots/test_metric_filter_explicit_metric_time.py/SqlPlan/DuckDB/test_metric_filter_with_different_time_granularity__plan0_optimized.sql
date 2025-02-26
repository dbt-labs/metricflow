test_name: test_metric_filter_with_different_time_granularity
test_filename: test_metric_filter_explicit_metric_time.py
docstring:
  Tests a query with a metric filter where the parent query has a different time granularity.
    
      This test verifies that the parent query's time granularity is respected even when the filter
      doesn't explicitly include metric_time in its group_by list.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , SUM(bookings) AS bookings
FROM (
  -- Join Standard Outputs
  SELECT
    subq_22.listing__listings AS listing__listings
    , subq_16.metric_time__day AS metric_time__day
    , subq_16.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_16
  LEFT OUTER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__listings']
    SELECT
      listing
      , SUM(listings) AS listing__listings
    FROM (
      -- Read Elements From Semantic Model 'listings_latest'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['listings', 'listing']
      SELECT
        listing_id AS listing
        , 1 AS listings
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_19
    GROUP BY
      listing
  ) subq_22
  ON
    subq_16.listing = subq_22.listing
) subq_23
WHERE listing__listings > 0
GROUP BY
  metric_time__day
