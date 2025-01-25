test_name: test_custom_offset_window
test_filename: test_custom_granularity.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings AS bookings_offset_one_martian_day
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    nr_subq_23.ds__day__lead AS metric_time__day
    , SUM(nr_subq_19.bookings) AS bookings
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
        WHEN DATEADD(day, (cte_6.ds__day__row_number - 1), nr_subq_22.ds__martian_day__first_value__lead) <= nr_subq_22.ds__martian_day__last_value__lead
          THEN DATEADD(day, (cte_6.ds__day__row_number - 1), nr_subq_22.ds__martian_day__first_value__lead)
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
      ) nr_subq_21
    ) nr_subq_22
    ON
      cte_6.ds__martian_day = nr_subq_22.ds__martian_day
  ) nr_subq_23
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_19
  ON
    nr_subq_23.ds__day = nr_subq_19.metric_time__day
  GROUP BY
    nr_subq_23.ds__day__lead
) nr_subq_29
