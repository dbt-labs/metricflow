test_name: test_derived_metric_with_offset_window_and_offset_to_grain_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  metric_time__year
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_27.metric_time__year, subq_36.metric_time__year) AS metric_time__year
    , MAX(subq_27.month_start_bookings) AS month_start_bookings
    , MAX(subq_36.bookings_1_month_ago) AS bookings_1_month_ago
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATETIME_TRUNC(time_spine_src_28006.ds, year) AS metric_time__year
      , SUM(subq_20.bookings) AS month_start_bookings
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_20
    ON
      DATETIME_TRUNC(time_spine_src_28006.ds, month) = subq_20.metric_time__day
    WHERE DATETIME_TRUNC(time_spine_src_28006.ds, year) = time_spine_src_28006.ds
    GROUP BY
      metric_time__year
  ) subq_27
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATETIME_TRUNC(time_spine_src_28006.ds, year) AS metric_time__year
      , SUM(subq_29.bookings) AS bookings_1_month_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_29
    ON
      DATE_SUB(CAST(time_spine_src_28006.ds AS DATETIME), INTERVAL 1 month) = subq_29.metric_time__day
    GROUP BY
      metric_time__year
  ) subq_36
  ON
    subq_27.metric_time__year = subq_36.metric_time__year
  GROUP BY
    metric_time__year
) subq_37
