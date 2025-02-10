test_name: test_offset_by_custom_granularity_node
test_filename: test_dataflow_to_sql_plan.py
sql_engine: DuckDB
---
-- Apply Requested Granularities
SELECT
  subq_4.ds__month
  , subq_4.ds__month__lead AS metric_time__month
FROM (
  -- Offset Base Granularity By Custom Granularity Period(s)
  WITH cte_0 AS (
    -- Get Custom Granularity Bounds
    SELECT
      subq_1.ds__month
      , subq_1.ds__alien_day
      , FIRST_VALUE(subq_1.ds__month) OVER (
        PARTITION BY subq_1.ds__alien_day
        ORDER BY subq_1.ds__month
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ds__month__first_value
      , LAST_VALUE(subq_1.ds__month) OVER (
        PARTITION BY subq_1.ds__alien_day
        ORDER BY subq_1.ds__month
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ds__month__last_value
      , ROW_NUMBER() OVER (
        PARTITION BY subq_1.ds__alien_day
        ORDER BY subq_1.ds__month
      ) AS ds__month__row_number
    FROM (
      -- Get Unique Rows for Grains
      SELECT
        subq_0.ds__month
        , subq_0.ds__alien_day
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
          , time_spine_src_28006.fiscal_quarter AS ds__fiscal_quarter
          , time_spine_src_28006.fiscal_year AS ds__fiscal_year
        FROM ***************************.mf_time_spine time_spine_src_28006
      ) subq_0
      GROUP BY
        subq_0.ds__month
        , subq_0.ds__alien_day
    ) subq_1
  )

  SELECT
    cte_0.ds__month AS ds__month
    , CASE
      WHEN subq_3.ds__month__first_value__lead + INTERVAL (cte_0.ds__month__row_number - 1) month <= subq_3.ds__month__last_value__lead
        THEN subq_3.ds__month__first_value__lead + INTERVAL (cte_0.ds__month__row_number - 1) month
      ELSE NULL
    END AS ds__month__lead
  FROM cte_0 cte_0
  INNER JOIN (
    -- Offset Custom Granularity Bounds
    SELECT
      subq_2.ds__alien_day
      , LEAD(subq_2.ds__month__first_value, 3) OVER (ORDER BY subq_2.ds__alien_day) AS ds__month__first_value__lead
      , LEAD(subq_2.ds__month__last_value, 3) OVER (ORDER BY subq_2.ds__alien_day) AS ds__month__last_value__lead
    FROM (
      -- Get Unique Rows for Custom Granularity Bounds
      SELECT
        cte_0.ds__alien_day
        , cte_0.ds__month__first_value
        , cte_0.ds__month__last_value
      FROM cte_0 cte_0
      GROUP BY
        cte_0.ds__alien_day
        , cte_0.ds__month__first_value
        , cte_0.ds__month__last_value
    ) subq_2
  ) subq_3
  ON
    cte_0.ds__alien_day = subq_3.ds__alien_day
) subq_4
