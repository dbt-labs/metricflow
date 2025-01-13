test_name: test_metric_time_only
test_filename: test_metric_time_without_metrics.py
docstring:
  Tests querying only metric time.
sql_engine: Clickhouse
---
-- Pass Only Elements: ['metric_time__day',]
SELECT
  subq_1.metric_time__day
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
    , subq_0.ds__martian_day
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
    , subq_0.ds__martian_day AS metric_time__martian_day
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    SELECT
      time_spine_src_28006.ds AS ds__day
      , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
      , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
      , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
      , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
      , EXTRACT(toYear FROM time_spine_src_28006.ds) AS ds__extract_year
      , EXTRACT(toQuarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
      , EXTRACT(toMonth FROM time_spine_src_28006.ds) AS ds__extract_month
      , EXTRACT(toDayOfMonth FROM time_spine_src_28006.ds) AS ds__extract_day
      , EXTRACT(toDayOfWeek FROM time_spine_src_28006.ds) AS ds__extract_dow
      , EXTRACT(toDayOfYear FROM time_spine_src_28006.ds) AS ds__extract_doy
      , time_spine_src_28006.martian_day AS ds__martian_day
    FROM ***************************.mf_time_spine time_spine_src_28006
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_0
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_1
GROUP BY
  subq_1.metric_time__day
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
