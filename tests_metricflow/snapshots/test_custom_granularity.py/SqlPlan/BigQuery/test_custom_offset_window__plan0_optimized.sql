test_name: test_custom_offset_window
test_filename: test_custom_granularity.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
WITH cgb_1_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  -- Calculate Custom Granularity Bounds
  SELECT
    ds AS ds__day
    , martian_day AS ds__martian_day
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
    subq_24.ds__day__lead AS metric_time__day
    , SUM(subq_17.bookings) AS bookings
  FROM (
    -- Offset Base Granularity By Custom Granularity Period(s)
    SELECT
      cgb_1_cte.ds__day AS ds__day
      , CASE
        WHEN DATE_ADD(CAST(subq_23.ds__martian_day__first_value__offset AS DATETIME), INTERVAL cgb_1_cte.ds__day__row_number - 1 day) <= subq_23.ds__martian_day__last_value__offset
          THEN DATE_ADD(CAST(subq_23.ds__martian_day__first_value__offset AS DATETIME), INTERVAL cgb_1_cte.ds__day__row_number - 1 day)
        ELSE subq_23.ds__martian_day__last_value__offset
      END AS ds__day__lead
    FROM cgb_1_cte cgb_1_cte
    INNER JOIN (
      -- Offset Custom Granularity Bounds
      SELECT
        ds__martian_day
        , LEAD(ds__martian_day__first_value, 1) OVER (ORDER BY ds__martian_day) AS ds__martian_day__first_value__offset
        , LEAD(ds__martian_day__last_value, 1) OVER (ORDER BY ds__martian_day) AS ds__martian_day__last_value__offset
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
  ) subq_24
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_17
  ON
    subq_24.ds__day = subq_17.metric_time__day
  GROUP BY
    metric_time__day
) subq_30
