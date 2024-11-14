test_name: test_query_with_ratio_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a ratio metric in the query-level where filter.
sql_engine: BigQuery
---
-- Read From CTE For node_id=cm_9
WITH cm_7_cte AS (
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
  ) subq_23
  GROUP BY
    listing
)

, cm_8_cte AS (
  -- Read From CTE For node_id=cm_7
  -- Compute Metrics via Expressions
  SELECT
    listing
    , CAST(bookings AS FLOAT64) / CAST(NULLIF(bookers, 0) AS FLOAT64) AS listing__bookings_per_booker
  FROM cm_7_cte cm_7_cte
)

, cm_9_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['listings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(listings) AS listings
  FROM (
    -- Join Standard Outputs
    SELECT
      cm_8_cte.listing__bookings_per_booker AS listing__bookings_per_booker
      , subq_20.listings AS listings
    FROM (
      -- Read Elements From Semantic Model 'listings_latest'
      -- Metric Time Dimension 'ds'
      SELECT
        listing_id AS listing
        , 1 AS listings
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_20
    LEFT OUTER JOIN
      cm_8_cte cm_8_cte
    ON
      subq_20.listing = cm_8_cte.listing
  ) subq_28
  WHERE listing__bookings_per_booker > 1
)

SELECT
  listings AS listings
FROM cm_9_cte cm_9_cte
