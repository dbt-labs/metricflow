test_name: test_derived_metric_with_offset_window_and_time_filter
test_filename: test_derived_metric_rendering.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_19.metric_time__day, nr_subq_28.metric_time__day) AS metric_time__day
    , MAX(nr_subq_19.bookings) AS bookings
    , MAX(nr_subq_28.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_15
    WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    GROUP BY
      metric_time__day
  ) nr_subq_19
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings_2_weeks_ago
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        time_spine_src_28006.ds AS metric_time__day
        , nr_subq_20.bookings AS bookings
      FROM ***************************.mf_time_spine time_spine_src_28006
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) nr_subq_20
      ON
        DATEADD(day, -14, time_spine_src_28006.ds) = nr_subq_20.metric_time__day
    ) nr_subq_24
    WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    GROUP BY
      metric_time__day
  ) nr_subq_28
  ON
    nr_subq_19.metric_time__day = nr_subq_28.metric_time__day
  GROUP BY
    COALESCE(nr_subq_19.metric_time__day, nr_subq_28.metric_time__day)
) nr_subq_29
