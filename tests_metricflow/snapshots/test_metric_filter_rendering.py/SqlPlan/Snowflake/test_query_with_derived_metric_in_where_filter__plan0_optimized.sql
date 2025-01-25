test_name: test_query_with_derived_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a derived metric in the query-level where filter.
sql_engine: Snowflake
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
    nr_subq_32.listing__views_times_booking_value AS listing__views_times_booking_value
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
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__views_times_booking_value']
    SELECT
      listing
      , booking_value * views AS listing__views_times_booking_value
    FROM (
      -- Combine Aggregated Outputs
      SELECT
        COALESCE(nr_subq_27.listing, nr_subq_29.listing) AS listing
        , MAX(nr_subq_27.booking_value) AS booking_value
        , MAX(nr_subq_29.views) AS views
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
      ) nr_subq_27
      FULL OUTER JOIN (
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
        ) nr_subq_15
        GROUP BY
          listing
      ) nr_subq_29
      ON
        nr_subq_27.listing = nr_subq_29.listing
      GROUP BY
        COALESCE(nr_subq_27.listing, nr_subq_29.listing)
    ) nr_subq_30
  ) nr_subq_32
  ON
    nr_subq_25.listing = nr_subq_32.listing
) nr_subq_33
WHERE listing__views_times_booking_value > 1
