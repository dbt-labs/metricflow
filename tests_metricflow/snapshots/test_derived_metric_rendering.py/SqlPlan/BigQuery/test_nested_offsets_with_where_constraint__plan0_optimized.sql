test_name: test_nested_offsets_with_where_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
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
  -- Constrain Output with WHERE
  SELECT
    metric_time__day
    , bookings_offset_once
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_24.bookings_offset_once AS bookings_offset_once
    FROM rss_28018_cte rss_28018_cte
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
          , SUM(subq_16.bookings) AS bookings
        FROM rss_28018_cte rss_28018_cte
        INNER JOIN (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATETIME_TRUNC(ds, day) AS metric_time__day
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_16
        ON
          DATE_SUB(CAST(rss_28018_cte.ds__day AS DATETIME), INTERVAL 5 day) = subq_16.metric_time__day
        GROUP BY
          metric_time__day
      ) subq_23
    ) subq_24
    ON
      DATE_SUB(CAST(rss_28018_cte.ds__day AS DATETIME), INTERVAL 2 day) = subq_24.metric_time__day
  ) subq_28
  WHERE metric_time__day = '2020-01-12' or metric_time__day = '2020-01-13'
) subq_29
