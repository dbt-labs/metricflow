test_name: test_min_max_metric_time_week
test_filename: test_query_rendering.py
docstring:
  Tests a plan to get the min & max distinct values of metric_time with non-default granularity.
sql_engine: Postgres
---
-- Write to DataTable
SELECT
  subq_4.metric_time__week__min
  , subq_4.metric_time__week__max
FROM (
  -- Calculate min and max
  SELECT
    MIN(subq_3.metric_time__week) AS metric_time__week__min
    , MAX(subq_3.metric_time__week) AS metric_time__week__max
  FROM (
    -- Pass Only Elements: ['metric_time__week']
    SELECT
      subq_2.metric_time__week
    FROM (
      -- Pass Only Elements: ['metric_time__week']
      SELECT
        subq_1.metric_time__week
      FROM (
        -- Metric Time Dimension 'ds'
        SELECT
          subq_0.ds__day
          , subq_0.ds__week
          , subq_0.ds__month
          , subq_0.ds__quarter
          , subq_0.ds__year
          , subq_0.ds__extract_year
          , subq_0.ds__extract_quarter
          , subq_0.ds__extract_month
          , subq_0.ds__extract_day
          , subq_0.ds__extract_dow
          , subq_0.ds__extract_doy
          , subq_0.ds__alien_day
          , subq_0.ds__day AS metric_time__day
          , subq_0.ds__week AS metric_time__week
          , subq_0.ds__month AS metric_time__month
          , subq_0.ds__quarter AS metric_time__quarter
          , subq_0.ds__year AS metric_time__year
          , subq_0.ds__extract_year AS metric_time__extract_year
          , subq_0.ds__extract_quarter AS metric_time__extract_quarter
          , subq_0.ds__extract_month AS metric_time__extract_month
          , subq_0.ds__extract_day AS metric_time__extract_day
          , subq_0.ds__extract_dow AS metric_time__extract_dow
          , subq_0.ds__extract_doy AS metric_time__extract_doy
          , subq_0.ds__alien_day AS metric_time__alien_day
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
      ) subq_1
    ) subq_2
    GROUP BY
      subq_2.metric_time__week
  ) subq_3
) subq_4
