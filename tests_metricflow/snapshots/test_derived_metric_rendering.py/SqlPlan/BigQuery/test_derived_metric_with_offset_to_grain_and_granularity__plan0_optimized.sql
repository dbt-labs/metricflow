test_name: test_derived_metric_with_offset_to_grain_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  metric_time__week
  , bookings - bookings_at_start_of_month AS bookings_growth_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_16.metric_time__week, nr_subq_24.metric_time__week) AS metric_time__week
    , MAX(nr_subq_16.bookings) AS bookings
    , MAX(nr_subq_24.bookings_at_start_of_month) AS bookings_at_start_of_month
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__week
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__week']
      SELECT
        DATETIME_TRUNC(ds, isoweek) AS metric_time__week
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_14
    GROUP BY
      metric_time__week
  ) nr_subq_16
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__week']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATETIME_TRUNC(time_spine_src_28006.ds, isoweek) AS metric_time__week
      , SUM(nr_subq_17.bookings) AS bookings_at_start_of_month
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , DATETIME_TRUNC(ds, isoweek) AS metric_time__week
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_17
    ON
      DATETIME_TRUNC(time_spine_src_28006.ds, month) = nr_subq_17.metric_time__day
    WHERE DATETIME_TRUNC(time_spine_src_28006.ds, isoweek) = time_spine_src_28006.ds
    GROUP BY
      metric_time__week
  ) nr_subq_24
  ON
    nr_subq_16.metric_time__week = nr_subq_24.metric_time__week
  GROUP BY
    metric_time__week
) nr_subq_25
