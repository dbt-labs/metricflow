test_name: test_offset_window_with_date_part
test_filename: test_granularity_date_part_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__extract_dow
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_19.metric_time__extract_dow, subq_28.metric_time__extract_dow) AS metric_time__extract_dow
    , MAX(subq_19.bookings) AS bookings
    , MAX(subq_28.bookings_2_weeks_ago) AS bookings_2_weeks_ago
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
        EXTRACT(isodow FROM ds) AS metric_time__extract_dow
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_17
    GROUP BY
      metric_time__extract_dow
  ) subq_19
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__extract_dow']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      EXTRACT(isodow FROM time_spine_src_28006.ds) AS metric_time__extract_dow
      , SUM(subq_21.bookings) AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_21
    ON
      time_spine_src_28006.ds - INTERVAL 14 day = subq_21.metric_time__day
    GROUP BY
      EXTRACT(isodow FROM time_spine_src_28006.ds)
  ) subq_28
  ON
    subq_19.metric_time__extract_dow = subq_28.metric_time__extract_dow
  GROUP BY
    COALESCE(subq_19.metric_time__extract_dow, subq_28.metric_time__extract_dow)
) subq_29
