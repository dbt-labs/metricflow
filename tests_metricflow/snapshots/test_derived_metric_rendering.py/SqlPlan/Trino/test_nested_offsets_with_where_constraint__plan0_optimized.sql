test_name: test_nested_offsets_with_where_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: Trino
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
  -- Constrain Output with WHERE
  SELECT
    metric_time__day
    , bookings_offset_once
  FROM (
    -- Join to Time Spine Dataset
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
        -- Compute Metrics via Expressions
        SELECT
          rss_28018_cte.ds__day AS metric_time__day
          , subq_19.bookings AS bookings
        FROM rss_28018_cte
        INNER JOIN (
          -- Aggregate Inputs for Simple Metrics
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
          ) subq_18
          GROUP BY
            metric_time__day
        ) subq_19
        ON
          DATE_ADD('day', -5, rss_28018_cte.ds__day) = subq_19.metric_time__day
      ) subq_24
    ) subq_25
    ON
      DATE_ADD('day', -2, rss_28018_cte.ds__day) = subq_25.metric_time__day
  ) subq_29
  WHERE metric_time__day = '2020-01-12' or metric_time__day = '2020-01-13'
) subq_30
