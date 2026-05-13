test_name: test_min_max_metric_time
test_filename: test_query_rendering.py
docstring:
  Tests a plan to get the min & max distinct values of metric_time.
sql_engine: ClickHouse
---
SELECT
  subq_4.metric_time__day__min
  , subq_4.metric_time__day__max
FROM (
  SELECT
    MIN(subq_3.metric_time__day) AS metric_time__day__min
    , MAX(subq_3.metric_time__day) AS metric_time__day__max
  FROM (
    SELECT
      subq_2.metric_time__day
    FROM (
      SELECT
        subq_1.metric_time__day
      FROM (
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
          SELECT
            time_spine_src_28006.ds AS ds__day
            , toStartOfWeek(time_spine_src_28006.ds, 1) AS ds__week
            , toStartOfMonth(time_spine_src_28006.ds) AS ds__month
            , toStartOfQuarter(time_spine_src_28006.ds) AS ds__quarter
            , toStartOfYear(time_spine_src_28006.ds) AS ds__year
            , toYear(time_spine_src_28006.ds) AS ds__extract_year
            , toQuarter(time_spine_src_28006.ds) AS ds__extract_quarter
            , toMonth(time_spine_src_28006.ds) AS ds__extract_month
            , toDayOfMonth(time_spine_src_28006.ds) AS ds__extract_day
            , toDayOfWeek(time_spine_src_28006.ds) AS ds__extract_dow
            , toDayOfYear(time_spine_src_28006.ds) AS ds__extract_doy
            , time_spine_src_28006.alien_day AS ds__alien_day
          FROM ***************************.mf_time_spine time_spine_src_28006
        ) subq_0
      ) subq_1
    ) subq_2
    GROUP BY
      subq_2.metric_time__day
  ) subq_3
) subq_4
