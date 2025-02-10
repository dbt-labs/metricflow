test_name: test_custom_offset_window_with_larger_grain
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , bookings AS bookings_last_fiscal_quarter
FROM (
  -- Join to Time Spine Dataset
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__month']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC('month', subq_26.ds__day__lead) AS metric_time__month
    , SUM(subq_22.bookings) AS bookings
  FROM (
    -- Offset Base Granularity By Custom Granularity Period(s)
    WITH cte_6 AS (
      -- Read From Time Spine 'mf_time_spine'
      -- Get Custom Granularity Bounds
      SELECT
        fiscal_quarter AS ds__fiscal_quarter
        , FIRST_VALUE(ds) OVER (
          PARTITION BY fiscal_quarter
          ORDER BY ds
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day__first_value
        , LAST_VALUE(ds) OVER (
          PARTITION BY fiscal_quarter
          ORDER BY ds
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day__last_value
        , ROW_NUMBER() OVER (
          PARTITION BY fiscal_quarter
          ORDER BY ds
        ) AS ds__day__row_number
      FROM ***************************.mf_time_spine time_spine_src_28006
    )

    SELECT
      CASE
        WHEN subq_25.ds__day__first_value__lead + INTERVAL (cte_6.ds__day__row_number - 1) day <= subq_25.ds__day__last_value__lead
          THEN subq_25.ds__day__first_value__lead + INTERVAL (cte_6.ds__day__row_number - 1) day
        ELSE NULL
      END AS ds__day__lead
    FROM cte_6 cte_6
    INNER JOIN (
      -- Offset Custom Granularity Bounds
      SELECT
        ds__fiscal_quarter
        , LEAD(ds__day__first_value, 1) OVER (ORDER BY ds__fiscal_quarter) AS ds__day__first_value__lead
        , LEAD(ds__day__last_value, 1) OVER (ORDER BY ds__fiscal_quarter) AS ds__day__last_value__lead
      FROM (
        -- Get Unique Rows for Custom Granularity Bounds
        SELECT
          ds__fiscal_quarter
          , ds__day__first_value
          , ds__day__last_value
        FROM cte_6 cte_6
        GROUP BY
          ds__fiscal_quarter
          , ds__day__first_value
          , ds__day__last_value
      ) subq_24
    ) subq_25
    ON
      cte_6.ds__fiscal_quarter = subq_25.ds__fiscal_quarter
  ) subq_26
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , DATE_TRUNC('month', ds) AS metric_time__month
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_22
  ON
    (
      subq_26.ds__day__lead = subq_22.metric_time__fiscal_quarter
    ) AND (
      DATE_TRUNC('month', subq_26.ds__day__lead) = subq_22.metric_time__month
    )
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_29
  ON
    subq_22.metric_time__day = subq_29.ds
  GROUP BY
    DATE_TRUNC('month', subq_26.ds__day__lead)
) subq_33
