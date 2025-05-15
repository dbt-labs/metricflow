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
      , alien_day AS ds__alien_day
      , FIRST_VALUE(ds) OVER (
        PARTITION BY alien_day
        ORDER BY ds
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ds__alien_day__first_value
      , LAST_VALUE(ds) OVER (
        PARTITION BY alien_day
        ORDER BY ds
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ds__alien_day__last_value
      , ROW_NUMBER() OVER (
        PARTITION BY alien_day
        ORDER BY ds
      ) AS ds__day__row_number
    FROM ***************************.mf_time_spine time_spine_src_28006
  )

  SELECT
    cte_2.ds__day AS ds__day
    , CASE
      WHEN subq_6.ds__alien_day__first_value__lead + INTERVAL (cte_2.ds__day__row_number - 1) day <= subq_6.ds__alien_day__last_value__lead
        THEN subq_6.ds__alien_day__first_value__lead + INTERVAL (cte_2.ds__day__row_number - 1) day
      ELSE NULL
    END AS ds__day__lead
  FROM cte_2
  INNER JOIN (
    -- Offset Custom Granularity Bounds
    SELECT
      ds__alien_day
      , LEAD(ds__alien_day__first_value, 3) OVER (ORDER BY ds__alien_day) AS ds__alien_day__first_value__lead
      , LEAD(ds__alien_day__last_value, 3) OVER (ORDER BY ds__alien_day) AS ds__alien_day__last_value__lead
    FROM (
      -- Get Unique Rows for Custom Granularity Bounds
      SELECT
        ds__alien_day
        , ds__alien_day__first_value
        , ds__alien_day__last_value
      FROM cte_2
      GROUP BY
        ds__alien_day
        , ds__alien_day__first_value
        , ds__alien_day__last_value
    ) subq_5
  ) subq_6
  ON
    cte_2.ds__alien_day = subq_6.ds__alien_day
) subq_7
