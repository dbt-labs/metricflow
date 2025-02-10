test_name: test_offset_by_custom_granularity_node
test_filename: test_dataflow_to_sql_plan.py
sql_engine: DuckDB
---
-- Apply Requested Granularities
SELECT
  ds__month
  , ds__month__lead AS metric_time__month
FROM (
  -- Offset Base Granularity By Custom Granularity Period(s)
  WITH cte_2 AS (
    -- Get Custom Granularity Bounds
    SELECT
      ds__month
      , ds__alien_day
      , FIRST_VALUE(ds__month) OVER (
        PARTITION BY ds__alien_day
        ORDER BY ds__month
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ds__month__first_value
      , LAST_VALUE(ds__month) OVER (
        PARTITION BY ds__alien_day
        ORDER BY ds__month
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ds__month__last_value
      , ROW_NUMBER() OVER (
        PARTITION BY ds__alien_day
        ORDER BY ds__month
      ) AS ds__month__row_number
    FROM (
      -- Read From Time Spine 'mf_time_spine'
      -- Get Unique Rows for Grains
      SELECT
        DATE_TRUNC('month', ds) AS ds__month
        , alien_day AS ds__alien_day
      FROM ***************************.mf_time_spine time_spine_src_28006
      GROUP BY
        DATE_TRUNC('month', ds)
        , alien_day
    ) subq_6
  )

  SELECT
    cte_2.ds__month AS ds__month
    , CASE
      WHEN subq_8.ds__month__first_value__lead + INTERVAL (cte_2.ds__month__row_number - 1) month <= subq_8.ds__month__last_value__lead
        THEN subq_8.ds__month__first_value__lead + INTERVAL (cte_2.ds__month__row_number - 1) month
      ELSE NULL
    END AS ds__month__lead
  FROM cte_2 cte_2
  INNER JOIN (
    -- Offset Custom Granularity Bounds
    SELECT
      ds__alien_day
      , LEAD(ds__month__first_value, 3) OVER (ORDER BY ds__alien_day) AS ds__month__first_value__lead
      , LEAD(ds__month__last_value, 3) OVER (ORDER BY ds__alien_day) AS ds__month__last_value__lead
    FROM (
      -- Get Unique Rows for Custom Granularity Bounds
      SELECT
        ds__alien_day
        , ds__month__first_value
        , ds__month__last_value
      FROM cte_2 cte_2
      GROUP BY
        ds__alien_day
        , ds__month__first_value
        , ds__month__last_value
    ) subq_7
  ) subq_8
  ON
    cte_2.ds__alien_day = subq_8.ds__alien_day
) subq_9
