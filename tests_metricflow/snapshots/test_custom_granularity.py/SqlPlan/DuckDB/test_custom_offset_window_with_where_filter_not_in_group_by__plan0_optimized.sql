test_name: test_custom_offset_window_with_where_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings AS bookings_offset_one_martian_day
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Join to Time Spine Dataset
    -- Join to Custom Granularity Dataset
    SELECT
      subq_27.ds__day__lead AS metric_time__day
      , subq_23.bookings AS bookings
      , subq_30.martian_day AS metric_time__martian_day
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
          ) AS ds__day__first_value
          , LAST_VALUE(ds) OVER (
            PARTITION BY martian_day
            ORDER BY ds
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS ds__day__last_value
          , ROW_NUMBER() OVER (
            PARTITION BY martian_day
            ORDER BY ds
          ) AS ds__day__row_number
        FROM ***************************.mf_time_spine time_spine_src_28006
      )

      SELECT
        cte_6.ds__day AS ds__day
        , CASE
          WHEN subq_26.ds__day__first_value__lead + INTERVAL (cte_6.ds__day__row_number - 1) day <= subq_26.ds__day__last_value__lead
            THEN subq_26.ds__day__first_value__lead + INTERVAL (cte_6.ds__day__row_number - 1) day
          ELSE NULL
        END AS ds__day__lead
      FROM cte_6 cte_6
      INNER JOIN (
        -- Offset Custom Granularity Bounds
        SELECT
          ds__martian_day
          , LEAD(ds__day__first_value, 1) OVER (ORDER BY ds__martian_day) AS ds__day__first_value__lead
          , LEAD(ds__day__last_value, 1) OVER (ORDER BY ds__martian_day) AS ds__day__last_value__lead
        FROM (
          -- Get Unique Rows for Custom Granularity Bounds
          SELECT
            ds__martian_day
            , ds__day__first_value
            , ds__day__last_value
          FROM cte_6 cte_6
          GROUP BY
            ds__martian_day
            , ds__day__first_value
            , ds__day__last_value
        ) subq_25
      ) subq_26
      ON
        cte_6.ds__martian_day = subq_26.ds__martian_day
    ) subq_27
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_23
    ON
      subq_27.ds__day = subq_23.metric_time__day
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_30
    ON
      subq_27.ds__day__lead = subq_30.ds
  ) subq_31
  WHERE metric_time__martian_day = '2020-01-01'
  GROUP BY
    metric_time__day
) subq_35
