test_name: test_offset_by_custom_granularity_node_with_smaller_grain
test_filename: test_dataflow_to_sql_plan.py
sql_engine: DuckDB
---
-- Apply Requested Granularities
SELECT
  ts__hour
  , ts__hour__lead AS metric_time__hour
FROM (
  -- Offset Base Granularity By Custom Granularity Period(s)
  WITH cte_2 AS (
    -- Get Custom Granularity Bounds
    SELECT
      time_spine_src_28005.ts AS ts__hour
      , time_spine_src_28006.martian_day AS ds__martian_day
      , FIRST_VALUE(time_spine_src_28005.ts) OVER (
        PARTITION BY time_spine_src_28006.martian_day
        ORDER BY time_spine_src_28005.ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ts__hour__first_value
      , LAST_VALUE(time_spine_src_28005.ts) OVER (
        PARTITION BY time_spine_src_28006.martian_day
        ORDER BY time_spine_src_28005.ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ts__hour__last_value
      , ROW_NUMBER() OVER (
        PARTITION BY time_spine_src_28006.martian_day
        ORDER BY time_spine_src_28005.ts
      ) AS ts__hour__row_number
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN
      ***************************.mf_time_spine_hour time_spine_src_28005
    ON
      time_spine_src_28006.ds = DATE_TRUNC('day', time_spine_src_28005.ts)
  )

  SELECT
    cte_2.ts__hour AS ts__hour
    , CASE
      WHEN subq_8.ts__hour__first_value__lead + INTERVAL (cte_2.ts__hour__row_number - 1) hour <= subq_8.ts__hour__last_value__lead
        THEN subq_8.ts__hour__first_value__lead + INTERVAL (cte_2.ts__hour__row_number - 1) hour
      ELSE NULL
    END AS ts__hour__lead
  FROM cte_2 cte_2
  INNER JOIN (
    -- Offset Custom Granularity Bounds
    SELECT
      ds__martian_day
      , LEAD(ts__hour__first_value, 5) OVER (ORDER BY ds__martian_day) AS ts__hour__first_value__lead
      , LEAD(ts__hour__last_value, 5) OVER (ORDER BY ds__martian_day) AS ts__hour__last_value__lead
    FROM (
      -- Get Unique Rows for Custom Granularity Bounds
      SELECT
        ds__martian_day
        , ts__hour__first_value
        , ts__hour__last_value
      FROM cte_2 cte_2
      GROUP BY
        ds__martian_day
        , ts__hour__first_value
        , ts__hour__last_value
    ) subq_7
  ) subq_8
  ON
    cte_2.ds__martian_day = subq_8.ds__martian_day
) subq_9
