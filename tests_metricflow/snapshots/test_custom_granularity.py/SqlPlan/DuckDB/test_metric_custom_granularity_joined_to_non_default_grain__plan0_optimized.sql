test_name: test_metric_custom_granularity_joined_to_non_default_grain
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Metric Time Dimension 'ds'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['__listings', 'metric_time__alien_day', 'listing__ds__month']
-- Pass Only Elements: ['__listings', 'metric_time__alien_day', 'listing__ds__month']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_8.alien_day AS metric_time__alien_day
  , subq_7.listing__ds__month AS listing__ds__month
  , SUM(subq_7.__listings) AS listings
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  SELECT
    1 AS __listings
    , DATE_TRUNC('day', created_at) AS ds__day
    , DATE_TRUNC('month', created_at) AS listing__ds__month
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_7
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_8
ON
  subq_7.ds__day = subq_8.ds
GROUP BY
  subq_8.alien_day
  , subq_7.listing__ds__month
