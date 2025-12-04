test_name: test_nested_offsets
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH rss_28018_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS ds__day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Join to Time Spine Dataset
  SELECT
    rss_28018_cte.ds__day AS metric_time__day
    , subq_29.bookings_offset_once AS bookings_offset_once
  FROM rss_28018_cte
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , 2 * bookings AS bookings_offset_once
    FROM (
      -- Join to Time Spine Dataset
      -- Compute Metrics via Expressions
      SELECT
        rss_28018_cte.ds__day AS metric_time__day
        , subq_22.__bookings AS bookings
      FROM rss_28018_cte
      INNER JOIN (
        -- Aggregate Inputs for Simple Metrics
        SELECT
          metric_time__day
          , SUM(__bookings) AS __bookings
        FROM (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['__bookings', 'metric_time__day']
          -- Pass Only Elements: ['__bookings', 'metric_time__day']
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , 1 AS __bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_21
        GROUP BY
          metric_time__day
      ) subq_22
      ON
        rss_28018_cte.ds__day - INTERVAL 5 day = subq_22.metric_time__day
    ) subq_28
  ) subq_29
  ON
    rss_28018_cte.ds__day - INTERVAL 2 day = subq_29.metric_time__day
) subq_34
