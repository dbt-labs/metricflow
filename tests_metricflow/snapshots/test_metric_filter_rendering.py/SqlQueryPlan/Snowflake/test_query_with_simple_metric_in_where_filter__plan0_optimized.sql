test_name: test_query_with_simple_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a simple metric in the query-level where filter.
sql_engine: Snowflake
---
-- Read From CTE For node_id=cm_4
WITH cm_3_cte AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
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
  ) subq_16
  GROUP BY
    listing
)

, cm_4_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['listings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(listings) AS listings
  FROM (
    -- Join Standard Outputs
    SELECT
      cm_3_cte.listing__bookings AS listing__bookings
      , subq_13.listings AS listings
    FROM (
      -- Read Elements From Semantic Model 'listings_latest'
      -- Metric Time Dimension 'ds'
      SELECT
        listing_id AS listing
        , 1 AS listings
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_13
    LEFT OUTER JOIN
      cm_3_cte cm_3_cte
    ON
      subq_13.listing = cm_3_cte.listing
  ) subq_20
  WHERE listing__bookings > 2
)

SELECT
  listings AS listings
FROM cm_4_cte cm_4_cte
