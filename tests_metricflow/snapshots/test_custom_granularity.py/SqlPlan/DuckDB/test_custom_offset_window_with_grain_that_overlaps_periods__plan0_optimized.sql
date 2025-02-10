test_name: test_custom_offset_window_with_grain_that_overlaps_periods
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__week
  , bookings AS bookings_last_fiscal_quarter
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__week']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_29.ds__week__lead AS metric_time__week
    , SUM(subq_24.bookings) AS bookings
  FROM (
    -- Offset Base Granularity By Custom Granularity Period(s)
    WITH cte_6 AS (
      -- Get Custom Granularity Bounds
      SELECT
        ds__week
        , ds__fiscal_quarter
        , FIRST_VALUE(ds__week) OVER (
          PARTITION BY ds__fiscal_quarter
          ORDER BY ds__week
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__week__first_value
        , LAST_VALUE(ds__week) OVER (
          PARTITION BY ds__fiscal_quarter
          ORDER BY ds__week
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__week__last_value
        , ROW_NUMBER() OVER (
          PARTITION BY ds__fiscal_quarter
          ORDER BY ds__week
        ) AS ds__week__row_number
      FROM (
        -- Read From Time Spine 'mf_time_spine'
        -- Get Unique Rows for Grains
        SELECT
          DATE_TRUNC('week', ds) AS ds__week
          , fiscal_quarter AS ds__fiscal_quarter
        FROM ***************************.mf_time_spine time_spine_src_28006
        GROUP BY
          DATE_TRUNC('week', ds)
          , fiscal_quarter
      ) subq_26
    )

    SELECT
      cte_6.ds__week AS ds__week
      , CASE
        WHEN subq_28.ds__week__first_value__lead + INTERVAL (cte_6.ds__week__row_number - 1) week <= subq_28.ds__week__last_value__lead
          THEN subq_28.ds__week__first_value__lead + INTERVAL (cte_6.ds__week__row_number - 1) week
        ELSE NULL
      END AS ds__week__lead
    FROM cte_6 cte_6
    INNER JOIN (
      -- Offset Custom Granularity Bounds
      SELECT
        ds__fiscal_quarter
        , LEAD(ds__week__first_value, 1) OVER (ORDER BY ds__fiscal_quarter) AS ds__week__first_value__lead
        , LEAD(ds__week__last_value, 1) OVER (ORDER BY ds__fiscal_quarter) AS ds__week__last_value__lead
      FROM (
        -- Get Unique Rows for Custom Granularity Bounds
        SELECT
          ds__fiscal_quarter
          , ds__week__first_value
          , ds__week__last_value
        FROM cte_6 cte_6
        GROUP BY
          ds__fiscal_quarter
          , ds__week__first_value
          , ds__week__last_value
      ) subq_27
    ) subq_28
    ON
      cte_6.ds__fiscal_quarter = subq_28.ds__fiscal_quarter
  ) subq_29
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('week', ds) AS metric_time__week
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_24
  ON
    subq_29.ds__week = subq_24.metric_time__week
  GROUP BY
    subq_29.ds__week__lead
) subq_35
