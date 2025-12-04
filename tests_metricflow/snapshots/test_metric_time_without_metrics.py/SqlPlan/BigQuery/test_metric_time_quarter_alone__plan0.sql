test_name: test_metric_time_quarter_alone
test_filename: test_metric_time_without_metrics.py
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_3.metric_time__quarter
FROM (
  -- Pass Only Elements: ['metric_time__quarter']
  SELECT
    subq_2.metric_time__quarter
  FROM (
    -- Pass Only Elements: ['metric_time__quarter']
    SELECT
      subq_1.metric_time__quarter
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
          , DATETIME_TRUNC(time_spine_src_28006.ds, isoweek) AS ds__week
          , DATETIME_TRUNC(time_spine_src_28006.ds, month) AS ds__month
          , DATETIME_TRUNC(time_spine_src_28006.ds, quarter) AS ds__quarter
          , DATETIME_TRUNC(time_spine_src_28006.ds, year) AS ds__year
          , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
          , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
          , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
          , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
          , IF(EXTRACT(dayofweek FROM time_spine_src_28006.ds) = 1, 7, EXTRACT(dayofweek FROM time_spine_src_28006.ds) - 1) AS ds__extract_dow
          , EXTRACT(dayofyear FROM time_spine_src_28006.ds) AS ds__extract_doy
          , time_spine_src_28006.alien_day AS ds__alien_day
        FROM ***************************.mf_time_spine time_spine_src_28006
      ) subq_0
    ) subq_1
  ) subq_2
  GROUP BY
    metric_time__quarter
) subq_3
