test_name: test_metric_custom_granularity_joined_to_non_default_grain
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Metric Time Dimension 'ds'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['listings', 'metric_time__martian_day', 'listing__ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_6.martian_day AS metric_time__martian_day
  , subq_5.listing__ds__month AS listing__ds__month
  , SUM(subq_5.listings) AS listings
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  SELECT
    1 AS listings
    , DATE_TRUNC('day', created_at) AS ds__day
    , DATE_TRUNC('month', created_at) AS listing__ds__month
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_5
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_6
ON
  subq_5.ds__day = subq_6.ds
GROUP BY
  subq_6.martian_day
  , subq_5.listing__ds__month
