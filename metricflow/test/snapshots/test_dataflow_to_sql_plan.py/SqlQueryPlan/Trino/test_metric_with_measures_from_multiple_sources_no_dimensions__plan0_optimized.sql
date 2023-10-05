-- Combine Metrics
-- Compute Metrics via Expressions
SELECT
  CAST(subq_15.bookings AS DOUBLE) / CAST(NULLIF(subq_20.listings, 0) AS DOUBLE) AS bookings_per_listing
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['bookings']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10001
) subq_15
CROSS JOIN (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['listings']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS listings
  FROM ***************************.dim_listings_latest listings_latest_src_10004
) subq_20
