test_name: test_offset_by_custom_granularity_node
test_filename: test_dataflow_to_sql_plan.py
sql_engine: DuckDB
---
-- Apply Requested Granularities
SELECT
  ds__day
  , DATE_TRUNC('month', ds__day__lead) AS metric_time__month
FROM (
  -- Offset Base Granularity By Custom Granularity Period(s)
  WITH cte_2 AS (
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
    cte_2.ds__day AS ds__day
    , CASE
      WHEN LEAD(subq_5.ds__martian_day__first_value, 3) OVER (ORDER BY subq_5.ds__martian_day) + INTERVAL (cte_2.ds__day__row_number - 1) day <= LEAD(subq_5.ds__martian_day__last_value, 3) OVER (ORDER BY subq_5.ds__martian_day)
        THEN LEAD(subq_5.ds__martian_day__first_value, 3) OVER (ORDER BY subq_5.ds__martian_day) + INTERVAL (cte_2.ds__day__row_number - 1) day
      ELSE NULL
    END AS ds__day__lead
  FROM cte_2 cte_2
  INNER JOIN (
    -- Get Unique Rows for Custom Granularity Bounds
    SELECT
      ds__martian_day
      , ds__martian_day__first_value
      , ds__martian_day__last_value
    FROM cte_2 cte_2
    GROUP BY
      ds__martian_day
      , ds__martian_day__first_value
      , ds__martian_day__last_value
  ) subq_5
  ON
    cte_2.ds__martian_day = subq_5.ds__martian_day
) subq_7
