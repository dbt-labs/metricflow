-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['listings', 'metric_time__martian_day', 'listing__ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_5.martian_day AS metric_time__martian_day
  , subq_4.listing__ds__month AS listing__ds__month
  , SUM(subq_4.listings) AS listings
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  SELECT
    1 AS listings
    , DATE_TRUNC('day', created_at) AS ds__day
    , DATE_TRUNC('month', created_at) AS listing__ds__month
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_4
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_5
ON
  subq_4.ds__day = subq_5.ds
GROUP BY
  subq_5.martian_day
  , subq_4.listing__ds__month
