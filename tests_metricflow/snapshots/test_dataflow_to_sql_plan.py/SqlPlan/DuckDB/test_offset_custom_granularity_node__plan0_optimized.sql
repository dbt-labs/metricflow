test_name: test_offset_custom_granularity_node
test_filename: test_dataflow_to_sql_plan.py
sql_engine: DuckDB
---
-- Join Offset Custom Granularity to Base Granularity
WITH cte_2 AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS ds__day
    , alien_day AS ds__alien_day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  cte_2.ds__day AS ds__day
  , subq_1.ds__alien_day__lead AS metric_time__alien_day
FROM cte_2
INNER JOIN (
  -- Offset Custom Granularity
  SELECT
    ds__alien_day
    , LEAD(ds__alien_day, 3) OVER (ORDER BY ds__alien_day) AS ds__alien_day__lead
  FROM cte_2
  GROUP BY
    ds__alien_day
) subq_1
ON
  cte_2.ds__alien_day = subq_1.ds__alien_day
