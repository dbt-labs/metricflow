-- Read Elements From Semantic Model 'listings_latest'
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['listings', 'user']
-- Join Standard Outputs
-- Pass Only Elements: ['listings', 'user__listing__user__average_booking_value']
-- Constrain Output with WHERE
-- Pass Only Elements: ['listings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(1) AS listings
FROM ***************************.dim_listings_latest listings_latest_src_28000
