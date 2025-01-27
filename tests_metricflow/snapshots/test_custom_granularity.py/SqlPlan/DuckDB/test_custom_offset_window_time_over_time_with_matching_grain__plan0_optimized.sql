test_name: test_custom_offset_window_time_over_time_with_matching_grain
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__martian_day AS metric_time__martian_day
  , bookings - bookings_offset / NULLIF(bookings_offset, 0) AS bookings_martian_day_over_martian_day
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_25.metric_time__martian_day, subq_30.metric_time__martian_day) AS metric_time__martian_day
    , MAX(subq_25.bookings_offset) AS bookings_offset
    , MAX(subq_30.bookings) AS bookings
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__martian_day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_20.metric_time__martian_day AS metric_time__martian_day
      , SUM(sma_28009_cte.bookings) AS bookings_offset
    FROM (
      -- Join Offset Custom Granularity to Base Granularity
      WITH cte_7 AS (
        -- Read From Time Spine 'mf_time_spine'
        SELECT
          ds AS ds__day
          , martian_day AS ds__martian_day
        FROM ***************************.mf_time_spine time_spine_src_28006
      )

      SELECT
        cte_7.ds__day AS ds__day
        , subq_19.ds__martian_day__lead AS metric_time__martian_day
      FROM cte_7 cte_7
      INNER JOIN (
        -- Offset Custom Granularity
        SELECT
          ds__martian_day
          , LEAD(ds__martian_day, 1) OVER (ORDER BY ds__martian_day) AS ds__martian_day__lead
        FROM cte_7 cte_7
        GROUP BY
          ds__martian_day
      ) subq_19
      ON
        cte_7.ds__martian_day = subq_19.ds__martian_day
    ) subq_20
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      subq_20.ds__day = sma_28009_cte.metric_time__day
    GROUP BY
      subq_20.metric_time__martian_day
  ) subq_25
  FULL OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__martian_day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_26.martian_day AS metric_time__martian_day
      , SUM(sma_28009_cte.bookings) AS bookings
    FROM sma_28009_cte sma_28009_cte
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_26
    ON
      sma_28009_cte.metric_time__day = subq_26.ds
    GROUP BY
      subq_26.martian_day
  ) subq_30
  ON
    subq_25.metric_time__martian_day = subq_30.metric_time__martian_day
  GROUP BY
    COALESCE(subq_25.metric_time__martian_day, subq_30.metric_time__martian_day)
) subq_31
