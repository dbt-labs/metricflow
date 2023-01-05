-- Join Aggregated Measures with Standard Outputs
-- Pass Only Elements:
--   ['bookings', 'listings']
-- Compute Metrics via Expressions
SELECT
  CAST(subq_13.bookings AS FLOAT64) / CAST(NULLIF(subq_17.listings, 0) AS FLOAT64) AS bookings_per_listing
FROM (
  -- Read Elements From Data Source 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['bookings']
  -- Aggregate Measures
  SELECT
    SUM(1) AS bookings
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10001
) subq_13
CROSS JOIN (
  -- Read Elements From Data Source 'listings_latest'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['listings']
  -- Aggregate Measures
  SELECT
    SUM(1) AS listings
  FROM ***************************.dim_listings_latest listings_latest_src_10004
) subq_17
