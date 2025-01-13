test_name: test_nested_offsets
test_filename: test_derived_metric_rendering.py
sql_engine: Clickhouse
---
-- Compute Metrics via Expressions
WITH rss_28018_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS ds__day
  FROM ***************************.mf_time_spine time_spine_src_28006
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
)

SELECT
  metric_time__day AS metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Join to Time Spine Dataset
  SELECT
    rss_28018_cte.ds__day AS metric_time__day
    , subq_23.bookings_offset_once AS bookings_offset_once
  FROM rss_28018_cte rss_28018_cte
  INNER JOIN
  (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , 2 * bookings AS bookings_offset_once
    FROM (
      -- Join to Time Spine Dataset
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        rss_28018_cte.ds__day AS metric_time__day
        , SUM(subq_15.bookings) AS bookings
      FROM rss_28018_cte rss_28018_cte
      INNER JOIN
      (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
        SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
      ) subq_15
      ON
        addDays(rss_28018_cte.ds__day, CAST(-5 AS Integer)) = subq_15.metric_time__day
      GROUP BY
        rss_28018_cte.ds__day
      SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
    ) subq_22
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_23
  ON
    addDays(rss_28018_cte.ds__day, CAST(-2 AS Integer)) = subq_23.metric_time__day
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_27
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
