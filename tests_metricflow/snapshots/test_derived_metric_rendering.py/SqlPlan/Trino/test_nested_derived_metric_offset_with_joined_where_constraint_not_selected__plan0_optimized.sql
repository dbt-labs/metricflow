test_name: test_nested_derived_metric_offset_with_joined_where_constraint_not_selected
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
  -- Pass Only Elements: ['metric_time__day', 'bookings_offset_once']
  SELECT
    metric_time__day
    , bookings_offset_once
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_26.booking__is_instant AS booking__is_instant
      , subq_26.bookings_offset_once AS bookings_offset_once
    FROM rss_28018_cte
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , booking__is_instant
        , 2 * bookings AS bookings_offset_once
      FROM (
        -- Join to Time Spine Dataset
        -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day']
        -- Aggregate Measures
        -- Compute Metrics via Expressions
        SELECT
          rss_28018_cte.ds__day AS metric_time__day
          , subq_18.booking__is_instant AS booking__is_instant
          , SUM(subq_18.bookings) AS bookings
        FROM rss_28018_cte
        INNER JOIN (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , is_instant AS booking__is_instant
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_18
        ON
          DATE_ADD('day', -5, rss_28018_cte.ds__day) = subq_18.metric_time__day
        GROUP BY
          rss_28018_cte.ds__day
          , subq_18.booking__is_instant
      ) subq_25
    ) subq_26
    ON
      DATE_ADD('day', -2, rss_28018_cte.ds__day) = subq_26.metric_time__day
  ) subq_30
  WHERE booking__is_instant
) subq_32
