test_name: test_query_with_derived_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a derived metric in the query-level where filter.
sql_engine: Postgres
---
-- Read From CTE For node_id=cm_8
WITH cm_5_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['booking_value', 'listing']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    listing_id AS listing
    , SUM(booking_value) AS booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    listing_id
)

, cm_6_cte AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    listing
    , SUM(views) AS views
  FROM (
    -- Read Elements From Semantic Model 'views_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['views', 'listing']
    SELECT
      listing_id AS listing
      , 1 AS views
    FROM ***************************.fct_views views_source_src_28000
  ) subq_28
  GROUP BY
    listing
)

, cm_7_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    listing
    , booking_value * views AS listing__views_times_booking_value
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_5_cte.listing, cm_6_cte.listing) AS listing
      , MAX(cm_5_cte.booking_value) AS booking_value
      , MAX(cm_6_cte.views) AS views
    FROM cm_5_cte cm_5_cte
    FULL OUTER JOIN
      cm_6_cte cm_6_cte
    ON
      cm_5_cte.listing = cm_6_cte.listing
    GROUP BY
      COALESCE(cm_5_cte.listing, cm_6_cte.listing)
  ) subq_31
)

, cm_8_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['listings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(listings) AS listings
  FROM (
    -- Join Standard Outputs
    SELECT
      cm_7_cte.listing__views_times_booking_value AS listing__views_times_booking_value
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
      cm_7_cte cm_7_cte
    ON
      subq_20.listing = cm_7_cte.listing
  ) subq_34
  WHERE listing__views_times_booking_value > 1
)

SELECT
  listings AS listings
FROM cm_8_cte cm_8_cte
