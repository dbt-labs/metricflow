test_name: test_subdaily_time_constraint_without_metrics
test_filename: test_granularity_date_part_rendering.py
sql_engine: Clickhouse
---
-- Pass Only Elements: ['metric_time__second',]
SELECT
  subq_2.metric_time__second
FROM (
  -- Constrain Time Range to [2020-01-01T00:00:02, 2020-01-01T00:00:08]
  SELECT
    subq_1.ts__second
    , subq_1.ts__minute
    , subq_1.ts__hour
    , subq_1.ts__day
    , subq_1.ts__week
    , subq_1.ts__month
    , subq_1.ts__quarter
    , subq_1.ts__year
    , subq_1.ts__extract_year
    , subq_1.ts__extract_quarter
    , subq_1.ts__extract_month
    , subq_1.ts__extract_day
    , subq_1.ts__extract_dow
    , subq_1.ts__extract_doy
    , subq_1.metric_time__second
    , subq_1.metric_time__minute
    , subq_1.metric_time__hour
    , subq_1.metric_time__day
    , subq_1.metric_time__week
    , subq_1.metric_time__month
    , subq_1.metric_time__quarter
    , subq_1.metric_time__year
    , subq_1.metric_time__extract_year
    , subq_1.metric_time__extract_quarter
    , subq_1.metric_time__extract_month
    , subq_1.metric_time__extract_day
    , subq_1.metric_time__extract_dow
    , subq_1.metric_time__extract_doy
  FROM (
    -- Metric Time Dimension 'ts'
    SELECT
      subq_0.ts__second
      , subq_0.ts__minute
      , subq_0.ts__hour
      , subq_0.ts__day
      , subq_0.ts__week
      , subq_0.ts__month
      , subq_0.ts__quarter
      , subq_0.ts__year
      , subq_0.ts__extract_year
      , subq_0.ts__extract_quarter
      , subq_0.ts__extract_month
      , subq_0.ts__extract_day
      , subq_0.ts__extract_dow
      , subq_0.ts__extract_doy
      , subq_0.ts__second AS metric_time__second
      , subq_0.ts__minute AS metric_time__minute
      , subq_0.ts__hour AS metric_time__hour
      , subq_0.ts__day AS metric_time__day
      , subq_0.ts__week AS metric_time__week
      , subq_0.ts__month AS metric_time__month
      , subq_0.ts__quarter AS metric_time__quarter
      , subq_0.ts__year AS metric_time__year
      , subq_0.ts__extract_year AS metric_time__extract_year
      , subq_0.ts__extract_quarter AS metric_time__extract_quarter
      , subq_0.ts__extract_month AS metric_time__extract_month
      , subq_0.ts__extract_day AS metric_time__extract_day
      , subq_0.ts__extract_dow AS metric_time__extract_dow
      , subq_0.ts__extract_doy AS metric_time__extract_doy
    FROM (
      -- Read From Time Spine 'mf_time_spine_second'
      SELECT
        time_spine_src_28003.ts AS ts__second
        , date_trunc('minute', time_spine_src_28003.ts) AS ts__minute
        , date_trunc('hour', time_spine_src_28003.ts) AS ts__hour
        , date_trunc('day', time_spine_src_28003.ts) AS ts__day
        , date_trunc('week', time_spine_src_28003.ts) AS ts__week
        , date_trunc('month', time_spine_src_28003.ts) AS ts__month
        , date_trunc('quarter', time_spine_src_28003.ts) AS ts__quarter
        , date_trunc('year', time_spine_src_28003.ts) AS ts__year
        , toYear(time_spine_src_28003.ts) AS ts__extract_year
        , toQuarter(time_spine_src_28003.ts) AS ts__extract_quarter
        , toMonth(time_spine_src_28003.ts) AS ts__extract_month
        , toDayOfMonth(time_spine_src_28003.ts) AS ts__extract_day
        , toDayOfWeek(time_spine_src_28003.ts) AS ts__extract_dow
        , toDayOfYear(time_spine_src_28003.ts) AS ts__extract_doy
      FROM ***************************.mf_time_spine_second time_spine_src_28003
    ) subq_0
  ) subq_1
  WHERE subq_1.metric_time__second BETWEEN '2020-01-01 00:00:02' AND '2020-01-01 00:00:08'
) subq_2
GROUP BY
  metric_time__second
