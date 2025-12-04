test_name: test_query_with_ratio_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a ratio metric in the query-level where filter.
sql_engine: Databricks
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
  -- Pass Only Elements: ['__listings', 'listing__bookings_per_booker']
  SELECT
    CAST(subq_40.bookings AS DOUBLE) / CAST(NULLIF(subq_40.bookers, 0) AS DOUBLE) AS listing__bookings_per_booker
    , subq_34.__listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , 1 AS __listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_34
  LEFT OUTER JOIN (
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      listing
      , SUM(__bookings) AS bookings
      , COUNT(DISTINCT __bookers) AS bookers
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['__bookings', '__bookers', 'listing']
      -- Pass Only Elements: ['__bookings', '__bookers', 'listing']
      SELECT
        listing_id AS listing
        , 1 AS __bookings
        , guest_id AS __bookers
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_38
    GROUP BY
      listing
  ) subq_40
  ON
    subq_34.listing = subq_40.listing
) subq_44
WHERE listing__bookings_per_booker > 1
