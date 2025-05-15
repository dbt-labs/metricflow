test_name: test_offset_custom_granularity_node
test_filename: test_dataflow_to_sql_plan.py
sql_engine: DuckDB
---
-- Join Offset Custom Granularity to Base Granularity
WITH cte_0 AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    time_spine_src_28006.ds AS ds__day
    , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
    , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
    , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
    , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
    , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
    , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
    , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
    , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
    , EXTRACT(isodow FROM time_spine_src_28006.ds) AS ds__extract_dow
    , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
    , time_spine_src_28006.alien_day AS ds__alien_day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  cte_0.ds__day AS ds__day
  , subq_0.ds__alien_day__lead AS metric_time__alien_day
FROM cte_0
INNER JOIN (
  -- Offset Custom Granularity
  SELECT
    cte_0.ds__alien_day
    , LEAD(cte_0.ds__alien_day, 3) OVER (ORDER BY cte_0.ds__alien_day) AS ds__alien_day__lead
  FROM cte_0
  GROUP BY
    cte_0.ds__alien_day
) subq_0
ON
  cte_0.ds__alien_day = subq_0.ds__alien_day
