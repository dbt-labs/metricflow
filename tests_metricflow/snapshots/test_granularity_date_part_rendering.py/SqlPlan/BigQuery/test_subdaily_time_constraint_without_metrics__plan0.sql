test_name: test_subdaily_time_constraint_without_metrics
test_filename: test_granularity_date_part_rendering.py
sql_engine: BigQuery
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
        , DATETIME_TRUNC(time_spine_src_28003.ts, minute) AS ts__minute
        , DATETIME_TRUNC(time_spine_src_28003.ts, hour) AS ts__hour
        , DATETIME_TRUNC(time_spine_src_28003.ts, day) AS ts__day
        , DATETIME_TRUNC(time_spine_src_28003.ts, isoweek) AS ts__week
        , DATETIME_TRUNC(time_spine_src_28003.ts, month) AS ts__month
        , DATETIME_TRUNC(time_spine_src_28003.ts, quarter) AS ts__quarter
        , DATETIME_TRUNC(time_spine_src_28003.ts, year) AS ts__year
        , EXTRACT(year FROM time_spine_src_28003.ts) AS ts__extract_year
        , EXTRACT(quarter FROM time_spine_src_28003.ts) AS ts__extract_quarter
        , EXTRACT(month FROM time_spine_src_28003.ts) AS ts__extract_month
        , EXTRACT(day FROM time_spine_src_28003.ts) AS ts__extract_day
        , IF(EXTRACT(dayofweek FROM time_spine_src_28003.ts) = 1, 7, EXTRACT(dayofweek FROM time_spine_src_28003.ts) - 1) AS ts__extract_dow
        , EXTRACT(dayofyear FROM time_spine_src_28003.ts) AS ts__extract_doy
      FROM ***************************.mf_time_spine_second time_spine_src_28003
    ) nr_subq_28016
  ) nr_subq_0
  WHERE nr_subq_0.metric_time__second BETWEEN '2020-01-01 00:00:02' AND '2020-01-01 00:00:08'
) nr_subq_1
GROUP BY
  metric_time__second
