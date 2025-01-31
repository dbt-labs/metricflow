test_name: test_custom_offset_window_time_over_time
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , DATE_TRUNC('week', ds) AS metric_time__week
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__week AS metric_time__week
  , bookings - bookings_offset / NULLIF(bookings_offset, 0) AS bookings_martian_day_over_martian_day
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_36.metric_time__week, subq_40.metric_time__week) AS metric_time__week
    , MAX(subq_36.bookings_offset) AS bookings_offset
    , MAX(subq_40.bookings) AS bookings
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__week']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('week', subq_30.ds__day__lead) AS metric_time__week
      , SUM(sma_28009_cte.bookings) AS bookings_offset
    FROM (
      -- Offset Base Granularity By Custom Granularity Period(s)
      WITH cte_7 AS (
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
        cte_7.ds__day AS ds__day
        , CASE
          WHEN subq_29.ds__day__first_value__lead + INTERVAL (cte_7.ds__day__row_number - 1) day <= subq_29.ds__day__last_value__lead
            THEN subq_29.ds__day__first_value__lead + INTERVAL (cte_7.ds__day__row_number - 1) day
          ELSE NULL
        END AS ds__day__lead
      FROM cte_7 cte_7
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
          FROM cte_7 cte_7
          GROUP BY
            ds__martian_day
            , ds__day__first_value
            , ds__day__last_value
        ) subq_28
      ) subq_29
      ON
        cte_7.ds__martian_day = subq_29.ds__martian_day
    ) subq_30
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      subq_30.ds__day = sma_28009_cte.metric_time__day
    GROUP BY
      DATE_TRUNC('week', subq_30.ds__day__lead)
  ) subq_36
  FULL OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['bookings', 'metric_time__week']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__week
      , SUM(bookings) AS bookings
    FROM sma_28009_cte sma_28009_cte
    GROUP BY
      metric_time__week
  ) subq_40
  ON
    subq_36.metric_time__week = subq_40.metric_time__week
  GROUP BY
    COALESCE(subq_36.metric_time__week, subq_40.metric_time__week)
) subq_41
