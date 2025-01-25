test_name: test_simple_metric_with_custom_granularity_and_join
test_filename: test_custom_granularity.py
sql_engine: Redshift
---
-- Join Standard Outputs
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['bookings', 'listing__ds__martian_day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  nr_subq_10.martian_day AS listing__ds__martian_day
  , SUM(nr_subq_7.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) nr_subq_7
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_28000
ON
  nr_subq_7.listing = listings_latest_src_28000.listing_id
LEFT OUTER JOIN
  ***************************.mf_time_spine nr_subq_10
ON
  DATE_TRUNC('day', listings_latest_src_28000.created_at) = nr_subq_10.ds
GROUP BY
  nr_subq_10.martian_day
