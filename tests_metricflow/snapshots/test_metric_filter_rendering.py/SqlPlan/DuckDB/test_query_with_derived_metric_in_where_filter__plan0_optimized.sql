test_name: test_query_with_derived_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a derived metric in the query-level where filter.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__listings']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__listings', 'listing__views_times_booking_value']
  SELECT
    subq_50.listing__views_times_booking_value AS listing__views_times_booking_value
    , subq_35.__listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , 1 AS __listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_35
  LEFT OUTER JOIN (
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__views_times_booking_value']
    SELECT
      listing
      , booking_value * views AS listing__views_times_booking_value
    FROM (
      -- Combine Aggregated Outputs
      SELECT
        COALESCE(subq_41.listing, subq_47.listing) AS listing
        , MAX(subq_41.booking_value) AS booking_value
        , MAX(subq_47.views) AS views
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['__booking_value', 'listing']
        -- Pass Only Elements: ['__booking_value', 'listing']
        -- Aggregate Inputs for Simple Metrics
        -- Compute Metrics via Expressions
        SELECT
          listing_id AS listing
          , SUM(booking_value) AS booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
        GROUP BY
          listing_id
      ) subq_41
      FULL OUTER JOIN (
        -- Aggregate Inputs for Simple Metrics
        -- Compute Metrics via Expressions
        SELECT
          listing
          , SUM(__views) AS views
        FROM (
          -- Read Elements From Semantic Model 'views_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['__views', 'listing']
          -- Pass Only Elements: ['__views', 'listing']
          SELECT
            listing_id AS listing
            , 1 AS __views
          FROM ***************************.fct_views views_source_src_28000
        ) subq_45
        GROUP BY
          listing
      ) subq_47
      ON
        subq_41.listing = subq_47.listing
      GROUP BY
        COALESCE(subq_41.listing, subq_47.listing)
    ) subq_48
  ) subq_50
  ON
    subq_35.listing = subq_50.listing
) subq_52
WHERE listing__views_times_booking_value > 1
