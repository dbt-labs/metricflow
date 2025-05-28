test_name: test_offset_by_custom_granularity_node
test_filename: test_dataflow_to_sql_plan.py
sql_engine: DuckDB
---
-- Apply Requested Granularities
SELECT
  subq_3.ds__day
  , DATE_TRUNC('month', subq_3.ds__day__lead) AS metric_time__month
FROM (
  -- Offset Base Granularity By Custom Granularity Period(s)
  WITH cte_0 AS (
    -- Get Custom Granularity Bounds
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
      , FIRST_VALUE(subq_0.ds__day) OVER (
        PARTITION BY subq_0.ds__alien_day
        ORDER BY subq_0.ds__day
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ds__alien_day__first_value
      , LAST_VALUE(subq_0.ds__day) OVER (
        PARTITION BY subq_0.ds__alien_day
        ORDER BY subq_0.ds__day
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ds__alien_day__last_value
      , ROW_NUMBER() OVER (
        PARTITION BY subq_0.ds__alien_day
        ORDER BY subq_0.ds__day
      ) AS ds__day__row_number
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
  )

  SELECT
    cte_0.ds__day AS ds__day
    , CASE
      WHEN subq_2.ds__alien_day__first_value__lead + INTERVAL (cte_0.ds__day__row_number - 1) day <= subq_2.ds__alien_day__last_value__lead
        THEN subq_2.ds__alien_day__first_value__lead + INTERVAL (cte_0.ds__day__row_number - 1) day
      ELSE NULL
    END AS ds__day__lead
  FROM cte_0
  INNER JOIN (
    -- Offset Custom Granularity Bounds
    SELECT
      subq_1.ds__alien_day
      , LEAD(subq_1.ds__alien_day__first_value, 3) OVER (ORDER BY subq_1.ds__alien_day) AS ds__alien_day__first_value__lead
      , LEAD(subq_1.ds__alien_day__last_value, 3) OVER (ORDER BY subq_1.ds__alien_day) AS ds__alien_day__last_value__lead
    FROM (
      -- Get Unique Rows for Custom Granularity Bounds
      SELECT
        cte_0.ds__alien_day
        , cte_0.ds__alien_day__first_value
        , cte_0.ds__alien_day__last_value
      FROM cte_0
      GROUP BY
        cte_0.ds__alien_day
        , cte_0.ds__alien_day__first_value
        , cte_0.ds__alien_day__last_value
    ) subq_1
  ) subq_2
  ON
    cte_0.ds__alien_day = subq_2.ds__alien_day
) subq_3
