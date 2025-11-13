test_name: test_multiple_metrics_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Postgres
---
-- Combine Aggregated Outputs
-- Write to DataTable
SELECT
  COALESCE(subq_18.metric_time__alien_day, subq_24.metric_time__alien_day) AS metric_time__alien_day
  , MAX(subq_18.bookings) AS bookings
  , MAX(subq_24.listings) AS listings
FROM (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['__bookings', 'metric_time__alien_day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    subq_14.alien_day AS metric_time__alien_day
    , SUM(subq_13.__bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS __bookings
      , DATE_TRUNC('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_13
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_14
  ON
    subq_13.ds__day = subq_14.ds
  GROUP BY
    subq_14.alien_day
) subq_18
FULL OUTER JOIN (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['__listings', 'metric_time__alien_day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    subq_20.alien_day AS metric_time__alien_day
    , SUM(subq_19.__listings) AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    SELECT
      1 AS __listings
      , DATE_TRUNC('day', created_at) AS ds__day
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_19
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_20
  ON
    subq_19.ds__day = subq_20.ds
  GROUP BY
    subq_20.alien_day
) subq_24
ON
  subq_18.metric_time__alien_day = subq_24.metric_time__alien_day
GROUP BY
  COALESCE(subq_18.metric_time__alien_day, subq_24.metric_time__alien_day)
