test_name: test_query_with_derived_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a derived metric in the query-level where filter.
sql_engine: Clickhouse
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
    subq_33.listing__views_times_booking_value AS listing__views_times_booking_value
    , subq_20.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_20
  LEFT OUTER JOIN
  (
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__views_times_booking_value']
    SELECT
      listing
      , booking_value * views AS listing__views_times_booking_value
    FROM (
      -- Combine Aggregated Outputs
      SELECT
        COALESCE(subq_25.listing, subq_30.listing) AS listing
        , MAX(subq_25.booking_value) AS booking_value
        , MAX(subq_30.views) AS views
      FROM (
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
        SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
      ) subq_25
      FULL OUTER JOIN
      (
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
          SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
        ) subq_28
        GROUP BY
          listing
        SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
      ) subq_30
      ON
        subq_25.listing = subq_30.listing
      GROUP BY
        COALESCE(subq_25.listing, subq_30.listing)
      SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
    ) subq_31
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_33
  ON
    subq_20.listing = subq_33.listing
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_34
WHERE listing__views_times_booking_value > 1
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
