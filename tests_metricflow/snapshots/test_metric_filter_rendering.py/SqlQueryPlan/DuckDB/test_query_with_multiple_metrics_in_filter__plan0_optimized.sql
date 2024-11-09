test_name: test_query_with_multiple_metrics_in_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with 2 simple metrics in the query-level where filter.
sql_engine: DuckDB
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
    subq_25.listing__bookings AS listing__bookings
    , subq_31.listing__bookers AS listing__bookers
    , subq_19.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_19
  LEFT OUTER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookings']
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
    ) subq_22
    GROUP BY
      listing
  ) subq_25
  ON
    subq_19.listing = subq_25.listing
  LEFT OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookers', 'listing']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookers']
    SELECT
      listing_id AS listing
      , COUNT(DISTINCT guest_id) AS listing__bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      listing_id
  ) subq_31
  ON
    subq_19.listing = subq_31.listing
) subq_32
WHERE listing__bookings > 2 AND listing__bookers > 1
