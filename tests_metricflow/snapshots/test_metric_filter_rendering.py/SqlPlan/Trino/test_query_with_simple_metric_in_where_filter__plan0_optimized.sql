test_name: test_query_with_simple_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a simple metric in the query-level where filter.
sql_engine: Trino
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  SELECT
    nr_subq_16.listing__bookings AS listing__bookings
    , nr_subq_13.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) nr_subq_13
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
    ) nr_subq_5
    GROUP BY
      listing
  ) nr_subq_16
  ON
    nr_subq_13.listing = nr_subq_16.listing
) nr_subq_17
WHERE listing__bookings > 2
