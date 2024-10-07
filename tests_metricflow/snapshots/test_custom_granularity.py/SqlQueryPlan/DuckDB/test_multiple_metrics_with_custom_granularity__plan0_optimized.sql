-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_14.metric_time__martian_day, subq_19.metric_time__martian_day) AS metric_time__martian_day
  , MAX(subq_14.bookings) AS bookings
  , MAX(subq_19.listings) AS listings
FROM (
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_11.martian_day AS metric_time__martian_day
    , SUM(subq_10.bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS bookings
      , DATE_TRUNC('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_10
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_11
  ON
    subq_10.ds__day = subq_11.ds
  GROUP BY
    subq_11.martian_day
) subq_14
FULL OUTER JOIN (
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['listings', 'metric_time__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_16.martian_day AS metric_time__martian_day
    , SUM(subq_15.listings) AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    SELECT
      1 AS listings
      , DATE_TRUNC('day', created_at) AS ds__day
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_15
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_16
  ON
    subq_15.ds__day = subq_16.ds
  GROUP BY
    subq_16.martian_day
) subq_19
ON
  subq_14.metric_time__martian_day = subq_19.metric_time__martian_day
GROUP BY
  COALESCE(subq_14.metric_time__martian_day, subq_19.metric_time__martian_day)
