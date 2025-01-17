test_name: test_multiple_metrics_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Clickhouse
---
-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_17.metric_time__martian_day, subq_23.metric_time__martian_day) AS metric_time__martian_day
  , MAX(subq_17.bookings) AS bookings
  , MAX(subq_23.listings) AS listings
FROM (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_13.martian_day AS metric_time__martian_day
    , SUM(subq_12.bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS bookings
      , date_trunc('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_12
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_13
  ON
    subq_12.ds__day = subq_13.ds
  GROUP BY
    metric_time__martian_day
) subq_17
FULL OUTER JOIN (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['listings', 'metric_time__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_19.martian_day AS metric_time__martian_day
    , SUM(subq_18.listings) AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    SELECT
      1 AS listings
      , date_trunc('day', created_at) AS ds__day
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_18
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_19
  ON
    subq_18.ds__day = subq_19.ds
  GROUP BY
    metric_time__martian_day
) subq_23
ON
  subq_17.metric_time__martian_day = subq_23.metric_time__martian_day
GROUP BY
  metric_time__martian_day
