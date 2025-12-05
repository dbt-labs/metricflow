test_name: test_multiple_metrics_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Postgres
---
-- Combine Aggregated Outputs
-- Write to DataTable
SELECT
  COALESCE(subq_21.metric_time__alien_day, subq_28.metric_time__alien_day) AS metric_time__alien_day
  , MAX(subq_21.bookings) AS bookings
  , MAX(subq_28.listings) AS listings
FROM (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['__bookings', 'metric_time__alien_day']
  -- Pass Only Elements: ['__bookings', 'metric_time__alien_day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    subq_16.alien_day AS metric_time__alien_day
    , SUM(subq_15.__bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS __bookings
      , DATE_TRUNC('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_15
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_16
  ON
    subq_15.ds__day = subq_16.ds
  GROUP BY
    subq_16.alien_day
) subq_21
FULL OUTER JOIN (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['__listings', 'metric_time__alien_day']
  -- Pass Only Elements: ['__listings', 'metric_time__alien_day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    subq_23.alien_day AS metric_time__alien_day
    , SUM(subq_22.__listings) AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    SELECT
      1 AS __listings
      , DATE_TRUNC('day', created_at) AS ds__day
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_22
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_23
  ON
    subq_22.ds__day = subq_23.ds
  GROUP BY
    subq_23.alien_day
) subq_28
ON
  subq_21.metric_time__alien_day = subq_28.metric_time__alien_day
GROUP BY
  COALESCE(subq_21.metric_time__alien_day, subq_28.metric_time__alien_day)
