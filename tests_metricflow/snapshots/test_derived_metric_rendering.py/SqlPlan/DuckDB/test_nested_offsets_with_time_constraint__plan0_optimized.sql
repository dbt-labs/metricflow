test_name: test_nested_offsets_with_time_constraint
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
  -- Constrain Time Range to [2020-01-12T00:00:00, 2020-01-13T00:00:00]
  SELECT
    rss_28018_cte.ds__day AS metric_time__day
    , subq_25.bookings_offset_once AS bookings_offset_once
  FROM rss_28018_cte
  INNER JOIN (
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
        , SUM(subq_17.bookings) AS bookings
      FROM rss_28018_cte
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_17
      ON
        rss_28018_cte.ds__day - INTERVAL 5 day = subq_17.metric_time__day
      GROUP BY
        rss_28018_cte.ds__day
    ) subq_24
  ) subq_25
  ON
    rss_28018_cte.ds__day - INTERVAL 2 day = subq_25.metric_time__day
  WHERE rss_28018_cte.ds__day BETWEEN '2020-01-12' AND '2020-01-13'
) subq_30
