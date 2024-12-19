test_name: test_custom_offset_window_with_granularity_and_date_part
test_filename: test_custom_granularity.py
sql_engine: Snowflake
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
  metric_time__martian_day AS metric_time__martian_day
  , booking__ds__month AS booking__ds__month
  , metric_time__extract_year AS metric_time__extract_year
  , bookings AS bookings_offset_one_martian_day
FROM (
  -- Join to Time Spine Dataset
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'booking__ds__month', 'metric_time__extract_year', 'metric_time__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_28.martian_day AS metric_time__martian_day
    , DATE_TRUNC('month', subq_25.ds__day__lead) AS booking__ds__month
    , EXTRACT(year FROM subq_25.ds__day__lead) AS metric_time__extract_year
    , SUM(subq_18.bookings) AS bookings
  FROM (
    -- Offset Base Granularity By Custom Granularity Period(s)
    SELECT
      cgb_1_cte.ds__day AS ds__day
      , CASE
        WHEN DATEADD(day, (cgb_1_cte.ds__day__row_number - 1), subq_24.ds__martian_day__first_value__offset) <= subq_24.ds__martian_day__last_value__offset
          THEN DATEADD(day, (cgb_1_cte.ds__day__row_number - 1), subq_24.ds__martian_day__first_value__offset)
        ELSE subq_24.ds__martian_day__last_value__offset
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
      ) subq_22
    ) subq_24
    ON
      cgb_1_cte.ds__martian_day = subq_24.ds__martian_day
  ) subq_25
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_18
  ON
    subq_25.ds__day = subq_18.metric_time__day
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_28
  ON
    subq_25.ds__day__lead = subq_28.ds
  GROUP BY
    subq_28.martian_day
    , DATE_TRUNC('month', subq_25.ds__day__lead)
    , EXTRACT(year FROM subq_25.ds__day__lead)
) subq_32
