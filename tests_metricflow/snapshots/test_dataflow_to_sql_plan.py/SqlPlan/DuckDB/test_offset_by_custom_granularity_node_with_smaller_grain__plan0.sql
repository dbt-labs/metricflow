test_name: test_offset_by_custom_granularity_node_with_smaller_grain
test_filename: test_dataflow_to_sql_plan.py
sql_engine: DuckDB
---
-- Apply Requested Granularities
SELECT
  subq_4.ts__hour
  , subq_4.ts__hour__lead AS metric_time__hour
FROM (
  -- Offset Base Granularity By Custom Granularity Period(s)
  WITH cte_0 AS (
    -- Get Custom Granularity Bounds
    SELECT
      subq_1.ts__hour AS ts__hour
      , subq_0.ds__alien_day AS ds__alien_day
      , FIRST_VALUE(subq_1.ts__hour) OVER (
        PARTITION BY subq_0.ds__alien_day
        ORDER BY subq_1.ts__hour
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ts__hour__first_value
      , LAST_VALUE(subq_1.ts__hour) OVER (
        PARTITION BY subq_0.ds__alien_day
        ORDER BY subq_1.ts__hour
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ts__hour__last_value
      , ROW_NUMBER() OVER (
        PARTITION BY subq_0.ds__alien_day
        ORDER BY subq_1.ts__hour
      ) AS ts__hour__row_number
    FROM (
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
    ) subq_0
    INNER JOIN (
      -- Read From Time Spine 'mf_time_spine_hour'
      SELECT
        time_spine_src_28005.ts AS ts__hour
        , DATE_TRUNC('day', time_spine_src_28005.ts) AS ts__day
        , DATE_TRUNC('week', time_spine_src_28005.ts) AS ts__week
        , DATE_TRUNC('month', time_spine_src_28005.ts) AS ts__month
        , DATE_TRUNC('quarter', time_spine_src_28005.ts) AS ts__quarter
        , DATE_TRUNC('year', time_spine_src_28005.ts) AS ts__year
        , EXTRACT(year FROM time_spine_src_28005.ts) AS ts__extract_year
        , EXTRACT(quarter FROM time_spine_src_28005.ts) AS ts__extract_quarter
        , EXTRACT(month FROM time_spine_src_28005.ts) AS ts__extract_month
        , EXTRACT(day FROM time_spine_src_28005.ts) AS ts__extract_day
        , EXTRACT(isodow FROM time_spine_src_28005.ts) AS ts__extract_dow
        , EXTRACT(doy FROM time_spine_src_28005.ts) AS ts__extract_doy
      FROM ***************************.mf_time_spine_hour time_spine_src_28005
    ) subq_1
    ON
      subq_0.ds__day = subq_1.ts__day
  )

  SELECT
    cte_0.ts__hour AS ts__hour
    , CASE
      WHEN subq_3.ts__hour__first_value__lead + INTERVAL (cte_0.ts__hour__row_number - 1) hour <= subq_3.ts__hour__last_value__lead
        THEN subq_3.ts__hour__first_value__lead + INTERVAL (cte_0.ts__hour__row_number - 1) hour
      ELSE NULL
    END AS ts__hour__lead
  FROM cte_0 cte_0
  INNER JOIN (
    -- Offset Custom Granularity Bounds
    SELECT
      subq_2.ds__alien_day
      , LEAD(subq_2.ts__hour__first_value, 5) OVER (ORDER BY subq_2.ds__alien_day) AS ts__hour__first_value__lead
      , LEAD(subq_2.ts__hour__last_value, 5) OVER (ORDER BY subq_2.ds__alien_day) AS ts__hour__last_value__lead
    FROM (
      -- Get Unique Rows for Custom Granularity Bounds
      SELECT
        cte_0.ds__alien_day
        , cte_0.ts__hour__first_value
        , cte_0.ts__hour__last_value
      FROM cte_0 cte_0
      GROUP BY
        cte_0.ds__alien_day
        , cte_0.ts__hour__first_value
        , cte_0.ts__hour__last_value
    ) subq_2
  ) subq_3
  ON
    cte_0.ds__alien_day = subq_3.ds__alien_day
) subq_4
