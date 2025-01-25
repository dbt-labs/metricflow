test_name: test_subdaily_time_constraint_without_metrics
test_filename: test_granularity_date_part_rendering.py
sql_engine: Snowflake
---
-- Pass Only Elements: ['metric_time__second',]
SELECT
  nr_subq_1.metric_time__second
FROM (
  -- Constrain Time Range to [2020-01-01T00:00:02, 2020-01-01T00:00:08]
  SELECT
    nr_subq_0.ts__second
    , nr_subq_0.ts__minute
    , nr_subq_0.ts__hour
    , nr_subq_0.ts__day
    , nr_subq_0.ts__week
    , nr_subq_0.ts__month
    , nr_subq_0.ts__quarter
    , nr_subq_0.ts__year
    , nr_subq_0.ts__extract_year
    , nr_subq_0.ts__extract_quarter
    , nr_subq_0.ts__extract_month
    , nr_subq_0.ts__extract_day
    , nr_subq_0.ts__extract_dow
    , nr_subq_0.ts__extract_doy
    , nr_subq_0.metric_time__second
    , nr_subq_0.metric_time__minute
    , nr_subq_0.metric_time__hour
    , nr_subq_0.metric_time__day
    , nr_subq_0.metric_time__week
    , nr_subq_0.metric_time__month
    , nr_subq_0.metric_time__quarter
    , nr_subq_0.metric_time__year
    , nr_subq_0.metric_time__extract_year
    , nr_subq_0.metric_time__extract_quarter
    , nr_subq_0.metric_time__extract_month
    , nr_subq_0.metric_time__extract_day
    , nr_subq_0.metric_time__extract_dow
    , nr_subq_0.metric_time__extract_doy
  FROM (
    -- Metric Time Dimension 'ts'
    SELECT
      nr_subq_28016.ts__second
      , nr_subq_28016.ts__minute
      , nr_subq_28016.ts__hour
      , nr_subq_28016.ts__day
      , nr_subq_28016.ts__week
      , nr_subq_28016.ts__month
      , nr_subq_28016.ts__quarter
      , nr_subq_28016.ts__year
      , nr_subq_28016.ts__extract_year
      , nr_subq_28016.ts__extract_quarter
      , nr_subq_28016.ts__extract_month
      , nr_subq_28016.ts__extract_day
      , nr_subq_28016.ts__extract_dow
      , nr_subq_28016.ts__extract_doy
      , nr_subq_28016.ts__second AS metric_time__second
      , nr_subq_28016.ts__minute AS metric_time__minute
      , nr_subq_28016.ts__hour AS metric_time__hour
      , nr_subq_28016.ts__day AS metric_time__day
      , nr_subq_28016.ts__week AS metric_time__week
      , nr_subq_28016.ts__month AS metric_time__month
      , nr_subq_28016.ts__quarter AS metric_time__quarter
      , nr_subq_28016.ts__year AS metric_time__year
      , nr_subq_28016.ts__extract_year AS metric_time__extract_year
      , nr_subq_28016.ts__extract_quarter AS metric_time__extract_quarter
      , nr_subq_28016.ts__extract_month AS metric_time__extract_month
      , nr_subq_28016.ts__extract_day AS metric_time__extract_day
      , nr_subq_28016.ts__extract_dow AS metric_time__extract_dow
      , nr_subq_28016.ts__extract_doy AS metric_time__extract_doy
    FROM (
      -- Read From Time Spine 'mf_time_spine_second'
      SELECT
        time_spine_src_28003.ts AS ts__second
        , DATE_TRUNC('minute', time_spine_src_28003.ts) AS ts__minute
        , DATE_TRUNC('hour', time_spine_src_28003.ts) AS ts__hour
        , DATE_TRUNC('day', time_spine_src_28003.ts) AS ts__day
        , DATE_TRUNC('week', time_spine_src_28003.ts) AS ts__week
        , DATE_TRUNC('month', time_spine_src_28003.ts) AS ts__month
        , DATE_TRUNC('quarter', time_spine_src_28003.ts) AS ts__quarter
        , DATE_TRUNC('year', time_spine_src_28003.ts) AS ts__year
        , EXTRACT(year FROM time_spine_src_28003.ts) AS ts__extract_year
        , EXTRACT(quarter FROM time_spine_src_28003.ts) AS ts__extract_quarter
        , EXTRACT(month FROM time_spine_src_28003.ts) AS ts__extract_month
        , EXTRACT(day FROM time_spine_src_28003.ts) AS ts__extract_day
        , EXTRACT(dayofweekiso FROM time_spine_src_28003.ts) AS ts__extract_dow
        , EXTRACT(doy FROM time_spine_src_28003.ts) AS ts__extract_doy
      FROM ***************************.mf_time_spine_second time_spine_src_28003
    ) nr_subq_28016
  ) nr_subq_0
  WHERE nr_subq_0.metric_time__second BETWEEN '2020-01-01 00:00:02' AND '2020-01-01 00:00:08'
) nr_subq_1
GROUP BY
  nr_subq_1.metric_time__second
