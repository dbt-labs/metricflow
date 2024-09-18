-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_20.metric_time__martian_day, subq_27.metric_time__martian_day) AS metric_time__martian_day
  , MAX(subq_20.bookings) AS bookings
  , MAX(subq_27.listings) AS listings
FROM (
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_16.martian_day AS metric_time__martian_day
    , SUM(subq_15.bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_15
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_16
  ON
    subq_15.metric_time__day = subq_16.ds
  GROUP BY
    metric_time__martian_day
) subq_20
FULL OUTER JOIN (
  -- Pass Only Elements: ['listings', 'metric_time__day']
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['listings', 'metric_time__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_23.martian_day AS metric_time__martian_day
    , SUM(subq_22.listings) AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(created_at, day) AS metric_time__day
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_22
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_23
  ON
    subq_22.metric_time__day = subq_23.ds
  GROUP BY
    metric_time__martian_day
) subq_27
ON
  subq_20.metric_time__martian_day = subq_27.metric_time__martian_day
GROUP BY
  metric_time__martian_day
