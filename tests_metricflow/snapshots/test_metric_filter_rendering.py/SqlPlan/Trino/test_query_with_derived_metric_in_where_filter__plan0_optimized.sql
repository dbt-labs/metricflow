test_name: test_query_with_derived_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a derived metric in the query-level where filter.
sql_engine: Trino
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
    subq_43.listing__views_times_booking_value AS listing__views_times_booking_value
    , subq_30.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_30
  LEFT OUTER JOIN (
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__views_times_booking_value']
    SELECT
      listing
      , booking_value * views AS listing__views_times_booking_value
    FROM (
      -- Combine Aggregated Outputs
      SELECT
        COALESCE(subq_35.listing, subq_40.listing) AS listing
        , MAX(subq_35.booking_value) AS booking_value
        , MAX(subq_40.views) AS views
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['booking_value', 'listing']
        -- Aggregate Inputs for Simple Metrics
        -- Compute Metrics via Expressions
        SELECT
          listing_id AS listing
          , SUM(booking_value) AS booking_value
        FROM ***************************.fct_bookings bookings_source_src_28000
        GROUP BY
          listing_id
      ) subq_35
      FULL OUTER JOIN (
        -- Aggregate Inputs for Simple Metrics
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
        ) subq_38
        GROUP BY
          listing
      ) subq_40
      ON
        subq_35.listing = subq_40.listing
      GROUP BY
        COALESCE(subq_35.listing, subq_40.listing)
    ) subq_41
  ) subq_43
  ON
    subq_30.listing = subq_43.listing
) subq_44
WHERE listing__views_times_booking_value > 1
