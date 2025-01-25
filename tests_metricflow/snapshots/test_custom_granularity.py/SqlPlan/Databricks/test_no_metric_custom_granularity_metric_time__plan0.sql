test_name: test_no_metric_custom_granularity_metric_time
test_filename: test_custom_granularity.py
sql_engine: Databricks
---
-- Pass Only Elements: ['metric_time__martian_day',]
SELECT
  nr_subq_1.metric_time__martian_day
FROM (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  SELECT
    nr_subq_28019.ds__day AS ds__day
    , nr_subq_28019.ds__week AS ds__week
    , nr_subq_28019.ds__month AS ds__month
    , nr_subq_28019.ds__quarter AS ds__quarter
    , nr_subq_28019.ds__year AS ds__year
    , nr_subq_28019.ds__extract_year AS ds__extract_year
    , nr_subq_28019.ds__extract_quarter AS ds__extract_quarter
    , nr_subq_28019.ds__extract_month AS ds__extract_month
    , nr_subq_28019.ds__extract_day AS ds__extract_day
    , nr_subq_28019.ds__extract_dow AS ds__extract_dow
    , nr_subq_28019.ds__extract_doy AS ds__extract_doy
    , nr_subq_28019.ds__martian_day AS ds__martian_day
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
    , nr_subq_0.martian_day AS metric_time__martian_day
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
      , EXTRACT(DAYOFWEEK_ISO FROM time_spine_src_28006.ds) AS ds__extract_dow
      , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
      , time_spine_src_28006.martian_day AS ds__martian_day
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) nr_subq_28019
  LEFT OUTER JOIN
    ***************************.mf_time_spine nr_subq_0
  ON
    nr_subq_28019.ds__day = nr_subq_0.ds
) nr_subq_1
GROUP BY
  nr_subq_1.metric_time__martian_day
