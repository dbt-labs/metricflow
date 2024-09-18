-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_17.metric_time__martian_day, subq_23.metric_time__martian_day) AS metric_time__martian_day
  , MAX(subq_17.bookings) AS bookings
  , MAX(subq_23.listings) AS listings
FROM (
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_14.martian_day AS metric_time__martian_day
    , SUM(subq_13.bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_13
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_14
  ON
    subq_13.metric_time__day = subq_14.ds
  GROUP BY
    metric_time__martian_day
) subq_17
FULL OUTER JOIN (
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['listings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_20.martian_day AS metric_time__martian_day
    , SUM(subq_19.listings) AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(created_at, day) AS metric_time__day
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_19
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_20
  ON
    subq_19.metric_time__day = subq_20.ds
  GROUP BY
    metric_time__martian_day
) subq_23
ON
  subq_17.metric_time__martian_day = subq_23.metric_time__martian_day
GROUP BY
  metric_time__martian_day
