test_name: test_metric_custom_granularity_joined_to_non_default_grain
test_filename: test_custom_granularity.py
sql_engine: Databricks
---
-- Metric Time Dimension 'ds'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['listings', 'metric_time__alien_day', 'listing__ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_7.alien_day AS metric_time__alien_day
  , subq_6.listing__ds__month AS listing__ds__month
  , SUM(subq_6.listings) AS listings
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  SELECT
    1 AS listings
    , DATE_TRUNC('day', created_at) AS ds__day
    , DATE_TRUNC('month', created_at) AS listing__ds__month
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_6
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_7
ON
  subq_6.ds__day = subq_7.ds
GROUP BY
  subq_7.alien_day
  , subq_6.listing__ds__month
