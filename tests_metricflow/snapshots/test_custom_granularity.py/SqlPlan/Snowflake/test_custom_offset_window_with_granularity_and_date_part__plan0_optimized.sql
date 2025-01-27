test_name: test_custom_offset_window_with_granularity_and_date_part
test_filename: test_custom_granularity.py
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
SELECT
  metric_time__martian_day
  , booking__ds__month
  , metric_time__extract_year
  , bookings AS bookings_offset_one_martian_day
FROM (
  -- Join to Time Spine Dataset
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'booking__ds__month', 'metric_time__extract_year', 'metric_time__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_21.martian_day AS metric_time__martian_day
    , DATE_TRUNC('month', subq_18.ds__day__lead) AS booking__ds__month
    , EXTRACT(year FROM subq_18.ds__day__lead) AS metric_time__extract_year
    , SUM(subq_14.bookings) AS bookings
  FROM (
    -- Offset Base Granularity By Custom Granularity Period(s)
    WITH cte_6 AS (
      -- Read From Time Spine 'mf_time_spine'
      -- Get Custom Granularity Bounds
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
      cte_6.ds__day AS ds__day
      , CASE
        WHEN DATEADD(day, (cte_6.ds__day__row_number - 1), subq_17.ds__martian_day__first_value__lead) <= subq_17.ds__martian_day__last_value__lead
          THEN DATEADD(day, (cte_6.ds__day__row_number - 1), subq_17.ds__martian_day__first_value__lead)
        ELSE NULL
      END AS ds__day__lead
    FROM cte_6 cte_6
    INNER JOIN (
      -- Offset Custom Granularity Bounds
      SELECT
        ds__martian_day
        , LEAD(ds__martian_day__first_value, 1) OVER (ORDER BY ds__martian_day) AS ds__martian_day__first_value__lead
        , LEAD(ds__martian_day__last_value, 1) OVER (ORDER BY ds__martian_day) AS ds__martian_day__last_value__lead
      FROM (
        -- Get Unique Rows for Custom Granularity Bounds
        SELECT
          ds__martian_day
          , ds__martian_day__first_value
          , ds__martian_day__last_value
        FROM cte_6 cte_6
        GROUP BY
          ds__martian_day
          , ds__martian_day__first_value
          , ds__martian_day__last_value
      ) subq_16
    ) subq_17
    ON
      cte_6.ds__martian_day = subq_17.ds__martian_day
  ) subq_18
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_14
  ON
    subq_18.ds__day = subq_14.metric_time__day
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_21
  ON
    subq_18.ds__day__lead = subq_21.ds
  GROUP BY
    subq_21.martian_day
    , DATE_TRUNC('month', subq_18.ds__day__lead)
    , EXTRACT(year FROM subq_18.ds__day__lead)
) subq_25
