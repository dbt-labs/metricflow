test_name: test_custom_offset_window_with_granularity_and_date_part
test_filename: test_custom_granularity.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__alien_day
  , booking__ds__month
  , metric_time__extract_year
  , bookings AS bookings_offset_one_alien_day
FROM (
  -- Join to Time Spine Dataset
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['__bookings', 'booking__ds__month', 'metric_time__extract_year', 'metric_time__alien_day']
  -- Pass Only Elements: ['__bookings', 'booking__ds__month', 'metric_time__extract_year', 'metric_time__alien_day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    subq_33.alien_day AS metric_time__alien_day
    , DATE_TRUNC('month', subq_29.ds__day__lead) AS booking__ds__month
    , EXTRACT(year FROM subq_29.ds__day__lead) AS metric_time__extract_year
    , SUM(subq_25.__bookings) AS bookings
  FROM (
    -- Offset Base Granularity By Custom Granularity Period(s)
    WITH cte_6 AS (
      -- Read From Time Spine 'mf_time_spine'
      -- Get Custom Granularity Bounds
      SELECT
        ds AS ds__day
        , alien_day AS ds__alien_day
        , FIRST_VALUE(ds) OVER (
          PARTITION BY alien_day
          ORDER BY ds
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__alien_day__first_value
        , LAST_VALUE(ds) OVER (
          PARTITION BY alien_day
          ORDER BY ds
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__alien_day__last_value
        , ROW_NUMBER() OVER (
          PARTITION BY alien_day
          ORDER BY ds
        ) AS ds__day__row_number
      FROM ***************************.mf_time_spine time_spine_src_28006
    )

    SELECT
      cte_6.ds__day AS ds__day
      , CASE
        WHEN DATEADD(day, (cte_6.ds__day__row_number - 1), subq_28.ds__alien_day__first_value__lead) <= subq_28.ds__alien_day__last_value__lead
          THEN DATEADD(day, (cte_6.ds__day__row_number - 1), subq_28.ds__alien_day__first_value__lead)
        ELSE NULL
      END AS ds__day__lead
    FROM cte_6
    INNER JOIN (
      -- Offset Custom Granularity Bounds
      SELECT
        ds__alien_day
        , LEAD(ds__alien_day__first_value, 1) OVER (ORDER BY ds__alien_day) AS ds__alien_day__first_value__lead
        , LEAD(ds__alien_day__last_value, 1) OVER (ORDER BY ds__alien_day) AS ds__alien_day__last_value__lead
      FROM (
        -- Get Unique Rows for Custom Granularity Bounds
        SELECT
          ds__alien_day
          , ds__alien_day__first_value
          , ds__alien_day__last_value
        FROM cte_6
        GROUP BY
          ds__alien_day
          , ds__alien_day__first_value
          , ds__alien_day__last_value
      ) subq_27
    ) subq_28
    ON
      cte_6.ds__alien_day = subq_28.ds__alien_day
  ) subq_29
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS __bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_25
  ON
    subq_29.ds__day = subq_25.metric_time__day
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_33
  ON
    subq_29.ds__day__lead = subq_33.ds
  GROUP BY
    subq_33.alien_day
    , DATE_TRUNC('month', subq_29.ds__day__lead)
    , EXTRACT(year FROM subq_29.ds__day__lead)
) subq_38
