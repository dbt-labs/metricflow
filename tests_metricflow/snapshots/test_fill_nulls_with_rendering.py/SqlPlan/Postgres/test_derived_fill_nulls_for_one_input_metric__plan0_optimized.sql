test_name: test_derived_fill_nulls_for_one_input_metric
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_24.metric_time__day, nr_subq_32.metric_time__day) AS metric_time__day
    , COALESCE(MAX(nr_subq_24.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , MAX(nr_subq_32.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        time_spine_src_28006.ds AS metric_time__day
        , nr_subq_19.bookings AS bookings
      FROM ***************************.mf_time_spine time_spine_src_28006
      LEFT OUTER JOIN (
        -- Aggregate Measures
        SELECT
          metric_time__day
          , SUM(bookings) AS bookings
        FROM (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['bookings', 'metric_time__day']
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) nr_subq_18
        GROUP BY
          metric_time__day
      ) nr_subq_19
      ON
        time_spine_src_28006.ds = nr_subq_19.metric_time__day
    ) nr_subq_23
  ) nr_subq_24
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , SUM(nr_subq_25.bookings) AS bookings_2_weeks_ago
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
      time_spine_src_28006.ds - MAKE_INTERVAL(days => 14) = nr_subq_25.metric_time__day
    GROUP BY
      time_spine_src_28006.ds
  ) nr_subq_32
  ON
    nr_subq_24.metric_time__day = nr_subq_32.metric_time__day
  GROUP BY
    COALESCE(nr_subq_24.metric_time__day, nr_subq_32.metric_time__day)
) nr_subq_33
