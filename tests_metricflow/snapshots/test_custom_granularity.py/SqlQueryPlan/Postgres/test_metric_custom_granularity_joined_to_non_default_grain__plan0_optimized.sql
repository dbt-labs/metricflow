-- Pass Only Elements: ['listings', 'metric_time__day', 'listing__ds__month']
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['listings', 'metric_time__martian_day', 'listing__ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_8.martian_day AS metric_time__martian_day
  , subq_7.listing__ds__month AS listing__ds__month
  , SUM(subq_7.listings) AS listings
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('month', created_at) AS listing__ds__month
    , DATE_TRUNC('day', created_at) AS metric_time__day
    , 1 AS listings
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_7
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_8
ON
  subq_7.metric_time__day = subq_8.ds
GROUP BY
  subq_8.martian_day
  , subq_7.listing__ds__month
