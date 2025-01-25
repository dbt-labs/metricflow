test_name: test_derived_metric_with_offset_window_and_offset_to_grain
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_24.metric_time__day, nr_subq_32.metric_time__day) AS metric_time__day
    , MAX(nr_subq_24.month_start_bookings) AS month_start_bookings
    , MAX(nr_subq_32.bookings_1_month_ago) AS bookings_1_month_ago
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , SUM(nr_subq_17.bookings) AS month_start_bookings
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_17
    ON
      DATE_TRUNC('month', time_spine_src_28006.ds) = nr_subq_17.metric_time__day
    GROUP BY
      time_spine_src_28006.ds
  ) nr_subq_24
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , SUM(nr_subq_25.bookings) AS bookings_1_month_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_25
    ON
      DATEADD(month, -1, time_spine_src_28006.ds) = nr_subq_25.metric_time__day
    GROUP BY
      time_spine_src_28006.ds
  ) nr_subq_32
  ON
    nr_subq_24.metric_time__day = nr_subq_32.metric_time__day
  GROUP BY
    COALESCE(nr_subq_24.metric_time__day, nr_subq_32.metric_time__day)
) nr_subq_33
