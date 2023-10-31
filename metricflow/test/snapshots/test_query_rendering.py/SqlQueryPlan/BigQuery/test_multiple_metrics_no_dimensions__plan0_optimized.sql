-- Combine Metrics
SELECT
  MAX(subq_17.bookings) AS bookings
  , MAX(subq_23.listings) AS listings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
  -- Pass Only Elements:
  --   ['bookings']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10001
  WHERE DATE_TRUNC(ds, day) BETWEEN '2020-01-01' AND '2020-01-01'
) subq_17
CROSS JOIN (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
  -- Pass Only Elements:
  --   ['listings']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS listings
  FROM ***************************.dim_listings_latest listings_latest_src_10004
  WHERE DATE_TRUNC(created_at, day) BETWEEN '2020-01-01' AND '2020-01-01'
) subq_23
