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
    subq_21.metric_time__day AS metric_time__day
    , SUM(subq_14.bookings) AS bookings
  FROM (
    -- Calculate Custom Granularity Bounds
    -- Calculate Custom Granularity Bounds
    -- Pass Only Elements: ['metric_time__day', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', CASE
        WHEN subq_18.ds__martian_day__first_value__offset + INTERVAL subq_16.ds__martian_day__row_number day <= subq_18.ds__martian_day__first_value__offset + INTERVAL subq_16.ds__martian_day__row_number day THEN subq_18.ds__martian_day__first_value__offset + INTERVAL subq_16.ds__martian_day__row_number day
        ELSE subq_18.ds__martian_day__last_value__offset
      END) AS metric_time__day
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Calculate Custom Granularity Bounds
      SELECT
        metric_time__martian_day
        , LAG(ds__martian_day__first_value, 1) OVER (
          ORDER BY metric_time__martian_day
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__martian_day__first_value__offset
        , LAG(ds__martian_day__last_value, 1) OVER (
          ORDER BY metric_time__martian_day
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__martian_day__last_value__offset
      FROM (
        -- Calculate Custom Granularity Bounds
        SELECT
          ds__martian_day__first_value
          , ds__martian_day__last_value
        FROM (
          -- Read From Time Spine 'mf_time_spine'
          -- Calculate Custom Granularity Bounds
          SELECT
            ds AS ds__day
            , DATE_TRUNC('week', ds) AS ds__week
            , DATE_TRUNC('month', ds) AS ds__month
            , DATE_TRUNC('quarter', ds) AS ds__quarter
            , DATE_TRUNC('year', ds) AS ds__year
            , EXTRACT(year FROM ds) AS ds__extract_year
            , EXTRACT(quarter FROM ds) AS ds__extract_quarter
            , EXTRACT(month FROM ds) AS ds__extract_month
            , EXTRACT(day FROM ds) AS ds__extract_day
            , EXTRACT(isodow FROM ds) AS ds__extract_dow
            , EXTRACT(doy FROM ds) AS ds__extract_doy
            , martian_day AS ds__martian_day
            , martian_day AS metric_time__martian_day
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
            ) AS ds__martian_day__row_number
          FROM ***************************.mf_time_spine time_spine_src_28006
        ) subq_16
        GROUP BY
          ds__martian_day__first_value
          , ds__martian_day__last_value
      ) subq_17
    ) subq_18
    ON
      subq_17.metric_time__martian_day = time_spine_src_28006.martian_day
  ) subq_21
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_14
  ON
    subq_21.metric_time__day = subq_14.metric_time__day
  GROUP BY
    subq_21.metric_time__day
) subq_25
