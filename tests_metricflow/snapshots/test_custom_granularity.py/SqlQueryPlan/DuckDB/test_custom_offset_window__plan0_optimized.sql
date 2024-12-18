test_name: test_custom_offset_window
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
WITH cgb_1_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  -- Calculate Custom Granularity Bounds
  SELECT
    martian_day AS ds__martian_day
    , FIRST_VALUE(ds) OVER (
      PARTITION BY martian_day
      ORDER BY ds
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS ds__martian_day__first_value
    , LAST_VALUE(ds) OVER (
      PARTITION BY martian_day
      ORDER BY ds
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS ds__martian_day__last_value
    , ROW_NUMBER() OVER (
      PARTITION BY martian_day
      ORDER BY ds
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS ds__day__row_number
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , bookings AS bookings_offset_one_martian_day
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_26.metric_time__day AS metric_time__day
    , SUM(subq_17.bookings) AS bookings
  FROM (
    -- Offset Base Granularity By Custom Granularity Period(s)
    -- Apply Requested Granularities
    -- Pass Only Elements: ['metric_time__day',]
    SELECT
      CASE
        WHEN subq_23.ds__martian_day__first_value__offset + INTERVAL (cgb_1_cte.ds__day__row_number - 1) day <= subq_23.ds__martian_day__last_value__offset
          THEN subq_23.ds__martian_day__first_value__offset + INTERVAL (cgb_1_cte.ds__day__row_number - 1) day
        ELSE subq_23.ds__martian_day__last_value__offset
      END AS metric_time__day
    FROM cgb_1_cte cgb_1_cte
    INNER JOIN (
      -- Offset Custom Granularity Bounds
      SELECT
        ds__martian_day
        , LAG(ds__martian_day__first_value, 1) OVER (
          ORDER BY ds__martian_day
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__martian_day__first_value__offset
        , LAG(ds__martian_day__last_value, 1) OVER (
          ORDER BY ds__martian_day
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__martian_day__last_value__offset
      FROM (
        -- Read From CTE For node_id=cgb_1
        -- Pass Only Elements: ['ds__martian_day', 'ds__martian_day__first_value', 'ds__martian_day__last_value']
        SELECT
          ds__martian_day__first_value
          , ds__martian_day__last_value
          , ds__martian_day
        FROM cgb_1_cte cgb_1_cte
        GROUP BY
          ds__martian_day__first_value
          , ds__martian_day__last_value
          , ds__martian_day
      ) subq_21
    ) subq_23
    ON
      cgb_1_cte.ds__martian_day = subq_23.ds__martian_day
  ) subq_26
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_17
  ON
    subq_26.metric_time__day = subq_17.metric_time__day
  GROUP BY
    subq_26.metric_time__day
) subq_30
