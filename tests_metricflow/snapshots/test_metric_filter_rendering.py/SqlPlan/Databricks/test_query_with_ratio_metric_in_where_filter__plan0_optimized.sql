test_name: test_query_with_ratio_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a ratio metric in the query-level where filter.
sql_engine: Databricks
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listings']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  SELECT
    CAST(subq_34.bookings AS DOUBLE) / CAST(NULLIF(subq_34.bookers, 0) AS DOUBLE) AS listing__bookings_per_booker
    , subq_29.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_29
  LEFT OUTER JOIN (
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      listing
      , SUM(bookings) AS bookings
      , COUNT(DISTINCT bookers) AS bookers
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'bookers', 'listing']
      SELECT
        listing_id AS listing
        , 1 AS bookings
        , guest_id AS bookers
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_32
    GROUP BY
      listing
  ) subq_34
  ON
    subq_29.listing = subq_34.listing
) subq_37
WHERE listing__bookings_per_booker > 1
