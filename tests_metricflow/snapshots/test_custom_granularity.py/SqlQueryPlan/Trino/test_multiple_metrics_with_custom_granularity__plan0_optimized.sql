test_name: test_multiple_metrics_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Combine Aggregated Outputs
WITH cm_4_cte AS (
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
      , DATE_TRUNC('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_12
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_13
  ON
    subq_12.ds__day = subq_13.ds
  GROUP BY
    subq_13.martian_day
)

, cm_5_cte AS (
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
      , DATE_TRUNC('day', created_at) AS ds__day
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_18
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_19
  ON
    subq_18.ds__day = subq_19.ds
  GROUP BY
    subq_19.martian_day
)

SELECT
  COALESCE(cm_4_cte.metric_time__martian_day, cm_5_cte.metric_time__martian_day) AS metric_time__martian_day
  , MAX(cm_4_cte.bookings) AS bookings
  , MAX(cm_5_cte.listings) AS listings
FROM cm_4_cte cm_4_cte
FULL OUTER JOIN
  cm_5_cte cm_5_cte
ON
  cm_4_cte.metric_time__martian_day = cm_5_cte.metric_time__martian_day
GROUP BY
  COALESCE(cm_4_cte.metric_time__martian_day, cm_5_cte.metric_time__martian_day)
