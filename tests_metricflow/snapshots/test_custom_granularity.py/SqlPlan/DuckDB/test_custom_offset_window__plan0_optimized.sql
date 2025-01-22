test_name: test_custom_offset_window
test_filename: test_custom_granularity.py
sql_engine: DuckDB
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
    subq_17.ds__day__lead AS metric_time__day
    , SUM(subq_13.bookings) AS bookings
  FROM (
    -- Offset Base Granularity By Custom Granularity Period(s)
    SELECT
      time_spine_src_28006.ds AS ds__day
      , CASE
        WHEN subq_16.ds__martian_day__first_value__offset + INTERVAL (subq_14.ds__day__row_number - 1) day <= subq_16.ds__martian_day__last_value__offset
          THEN subq_16.ds__martian_day__first_value__offset + INTERVAL (subq_14.ds__day__row_number - 1) day
        ELSE NULL
      END AS ds__day__lead
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Offset Custom Granularity Bounds
      SELECT
        ds__martian_day
        , LEAD(ds__martian_day__first_value, 1) OVER (ORDER BY ds__martian_day) AS ds__martian_day__first_value__offset
        , LEAD(ds__martian_day__last_value, 1) OVER (ORDER BY ds__martian_day) AS ds__martian_day__last_value__offset
      FROM (
        -- Get Unique Rows for Custom Granularity Bounds
        WITH cte_6 AS (
          -- Read From Time Spine 'mf_time_spine'
          -- Get Custom Granularity Bounds
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
          FROM ***************************.mf_time_spine time_spine_src_28006
        )

        SELECT
          ds__martian_day AS ds__martian_day
          , ds__martian_day__first_value AS ds__martian_day__first_value
          , ds__martian_day__last_value AS ds__martian_day__last_value
        FROM cte_6 cte_6
        GROUP BY
          ds__martian_day
          , ds__martian_day__first_value
          , ds__martian_day__last_value
      ) subq_15
    ) subq_16
    ON
      cte_6.ds__martian_day = subq_16.ds__martian_day
  ) subq_17
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_13
  ON
    subq_17.ds__day = subq_13.metric_time__day
  GROUP BY
    subq_17.ds__day__lead
) subq_23
