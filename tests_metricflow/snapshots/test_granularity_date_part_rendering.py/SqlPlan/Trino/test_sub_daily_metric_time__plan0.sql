test_name: test_sub_daily_metric_time
test_filename: test_granularity_date_part_rendering.py
sql_engine: Trino
---
-- Write to DataTable
SELECT
  subq_3.metric_time__millisecond
FROM (
  -- Pass Only Elements: ['metric_time__millisecond']
  SELECT
    subq_2.metric_time__millisecond
  FROM (
    -- Pass Only Elements: ['metric_time__millisecond']
    SELECT
      subq_1.metric_time__millisecond
    FROM (
      -- Metric Time Dimension 'ts'
      SELECT
        subq_0.ts__millisecond
        , subq_0.ts__second
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
        , subq_0.ts__millisecond AS metric_time__millisecond
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
          , EXTRACT(DAY_OF_WEEK FROM time_spine_src_28002.ts) AS ts__extract_dow
          , EXTRACT(doy FROM time_spine_src_28002.ts) AS ts__extract_doy
        FROM ***************************.mf_time_spine_millisecond time_spine_src_28002
      ) subq_0
    ) subq_1
  ) subq_2
  GROUP BY
    subq_2.metric_time__millisecond
) subq_3
