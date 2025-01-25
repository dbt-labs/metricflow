test_name: test_sub_daily_metric_time
test_filename: test_granularity_date_part_rendering.py
sql_engine: Postgres
---
-- Pass Only Elements: ['metric_time__millisecond',]
SELECT
  nr_subq_0.metric_time__millisecond
FROM (
  -- Metric Time Dimension 'ts'
  SELECT
    nr_subq_28015.ts__millisecond
    , nr_subq_28015.ts__second
    , nr_subq_28015.ts__minute
    , nr_subq_28015.ts__hour
    , nr_subq_28015.ts__day
    , nr_subq_28015.ts__week
    , nr_subq_28015.ts__month
    , nr_subq_28015.ts__quarter
    , nr_subq_28015.ts__year
    , nr_subq_28015.ts__extract_year
    , nr_subq_28015.ts__extract_quarter
    , nr_subq_28015.ts__extract_month
    , nr_subq_28015.ts__extract_day
    , nr_subq_28015.ts__extract_dow
    , nr_subq_28015.ts__extract_doy
    , nr_subq_28015.ts__millisecond AS metric_time__millisecond
    , nr_subq_28015.ts__second AS metric_time__second
    , nr_subq_28015.ts__minute AS metric_time__minute
    , nr_subq_28015.ts__hour AS metric_time__hour
    , nr_subq_28015.ts__day AS metric_time__day
    , nr_subq_28015.ts__week AS metric_time__week
    , nr_subq_28015.ts__month AS metric_time__month
    , nr_subq_28015.ts__quarter AS metric_time__quarter
    , nr_subq_28015.ts__year AS metric_time__year
    , nr_subq_28015.ts__extract_year AS metric_time__extract_year
    , nr_subq_28015.ts__extract_quarter AS metric_time__extract_quarter
    , nr_subq_28015.ts__extract_month AS metric_time__extract_month
    , nr_subq_28015.ts__extract_day AS metric_time__extract_day
    , nr_subq_28015.ts__extract_dow AS metric_time__extract_dow
    , nr_subq_28015.ts__extract_doy AS metric_time__extract_doy
  FROM (
    -- Read From Time Spine 'mf_time_spine_millisecond'
    SELECT
      time_spine_src_28002.ts AS ts__millisecond
      , DATE_TRUNC('second', time_spine_src_28002.ts) AS ts__second
      , DATE_TRUNC('minute', time_spine_src_28002.ts) AS ts__minute
      , DATE_TRUNC('hour', time_spine_src_28002.ts) AS ts__hour
      , DATE_TRUNC('day', time_spine_src_28002.ts) AS ts__day
      , DATE_TRUNC('week', time_spine_src_28002.ts) AS ts__week
      , DATE_TRUNC('month', time_spine_src_28002.ts) AS ts__month
      , DATE_TRUNC('quarter', time_spine_src_28002.ts) AS ts__quarter
      , DATE_TRUNC('year', time_spine_src_28002.ts) AS ts__year
      , EXTRACT(year FROM time_spine_src_28002.ts) AS ts__extract_year
      , EXTRACT(quarter FROM time_spine_src_28002.ts) AS ts__extract_quarter
      , EXTRACT(month FROM time_spine_src_28002.ts) AS ts__extract_month
      , EXTRACT(day FROM time_spine_src_28002.ts) AS ts__extract_day
      , EXTRACT(isodow FROM time_spine_src_28002.ts) AS ts__extract_dow
      , EXTRACT(doy FROM time_spine_src_28002.ts) AS ts__extract_doy
    FROM ***************************.mf_time_spine_millisecond time_spine_src_28002
  ) nr_subq_28015
) nr_subq_0
GROUP BY
  nr_subq_0.metric_time__millisecond
