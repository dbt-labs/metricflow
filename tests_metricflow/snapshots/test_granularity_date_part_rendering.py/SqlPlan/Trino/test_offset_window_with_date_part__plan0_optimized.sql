test_name: test_offset_window_with_date_part
test_filename: test_granularity_date_part_rendering.py
sql_engine: Trino
---
-- Compute Metrics via Expressions
SELECT
  metric_time__extract_dow
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_16.metric_time__extract_dow, nr_subq_24.metric_time__extract_dow) AS metric_time__extract_dow
    , MAX(nr_subq_16.bookings) AS bookings
    , MAX(nr_subq_24.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__extract_dow
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__extract_dow']
      SELECT
        EXTRACT(DAY_OF_WEEK FROM ds) AS metric_time__extract_dow
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_14
    GROUP BY
      metric_time__extract_dow
  ) nr_subq_16
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__extract_dow']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      EXTRACT(DAY_OF_WEEK FROM time_spine_src_28006.ds) AS metric_time__extract_dow
      , SUM(nr_subq_17.bookings) AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , EXTRACT(DAY_OF_WEEK FROM ds) AS metric_time__extract_dow
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_17
    ON
      DATE_ADD('day', -14, time_spine_src_28006.ds) = nr_subq_17.metric_time__day
    GROUP BY
      EXTRACT(DAY_OF_WEEK FROM time_spine_src_28006.ds)
  ) nr_subq_24
  ON
    nr_subq_16.metric_time__extract_dow = nr_subq_24.metric_time__extract_dow
  GROUP BY
    COALESCE(nr_subq_16.metric_time__extract_dow, nr_subq_24.metric_time__extract_dow)
) nr_subq_25
