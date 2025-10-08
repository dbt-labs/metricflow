test_name: test_simple_metric_with_custom_granularity_and_join
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Join Standard Outputs
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['bookings', 'listing__ds__alien_day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_15.alien_day AS listing__ds__alien_day
  , SUM(subq_11.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_11
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_28000
ON
  subq_11.listing = listings_latest_src_28000.listing_id
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_15
ON
  DATE_TRUNC('day', listings_latest_src_28000.created_at) = subq_15.ds
GROUP BY
  subq_15.alien_day
