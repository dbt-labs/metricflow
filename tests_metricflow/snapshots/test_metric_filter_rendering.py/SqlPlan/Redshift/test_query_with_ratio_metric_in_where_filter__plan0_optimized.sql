test_name: test_query_with_ratio_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a ratio metric in the query-level where filter.
sql_engine: Redshift
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
    CAST(nr_subq_30.bookings AS DOUBLE PRECISION) / CAST(NULLIF(nr_subq_30.bookers, 0) AS DOUBLE PRECISION) AS listing__bookings_per_booker
    , nr_subq_25.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) nr_subq_25
  LEFT OUTER JOIN (
    -- Aggregate Measures
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
    ) nr_subq_28
    GROUP BY
      listing
  ) nr_subq_30
  ON
    nr_subq_25.listing = nr_subq_30.listing
) nr_subq_33
WHERE listing__bookings_per_booker > 1
