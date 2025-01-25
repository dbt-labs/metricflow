test_name: test_metric_time_only
test_filename: test_metric_time_without_metrics.py
docstring:
  Tests querying only metric time.
sql_engine: BigQuery
---
-- Pass Only Elements: ['metric_time__day',]
SELECT
  nr_subq_0.metric_time__day
FROM (
  -- Metric Time Dimension 'ds'
  SELECT
    nr_subq_28019.ds__day
    , nr_subq_28019.ds__week
    , nr_subq_28019.ds__month
    , nr_subq_28019.ds__quarter
    , nr_subq_28019.ds__year
    , nr_subq_28019.ds__extract_year
    , nr_subq_28019.ds__extract_quarter
    , nr_subq_28019.ds__extract_month
    , nr_subq_28019.ds__extract_day
    , nr_subq_28019.ds__extract_dow
    , nr_subq_28019.ds__extract_doy
    , nr_subq_28019.ds__martian_day
    , nr_subq_28019.ds__day AS metric_time__day
    , nr_subq_28019.ds__week AS metric_time__week
    , nr_subq_28019.ds__month AS metric_time__month
    , nr_subq_28019.ds__quarter AS metric_time__quarter
    , nr_subq_28019.ds__year AS metric_time__year
    , nr_subq_28019.ds__extract_year AS metric_time__extract_year
    , nr_subq_28019.ds__extract_quarter AS metric_time__extract_quarter
    , nr_subq_28019.ds__extract_month AS metric_time__extract_month
    , nr_subq_28019.ds__extract_day AS metric_time__extract_day
    , nr_subq_28019.ds__extract_dow AS metric_time__extract_dow
    , nr_subq_28019.ds__extract_doy AS metric_time__extract_doy
    , nr_subq_28019.ds__martian_day AS metric_time__martian_day
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
      , time_spine_src_28006.martian_day AS ds__martian_day
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) nr_subq_28019
) nr_subq_0
GROUP BY
  metric_time__day
