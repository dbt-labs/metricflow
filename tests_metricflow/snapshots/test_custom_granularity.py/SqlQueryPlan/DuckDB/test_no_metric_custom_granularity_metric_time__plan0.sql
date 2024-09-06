-- Pass Only Elements: ['metric_time__martian_day',]
SELECT
  subq_2.metric_time__martian_day
FROM (
  -- Join to Custom Granularity Dataset
  -- Metric Time Dimension 'ts'
  SELECT
    subq_0.ts__hour AS ts__hour
    , subq_0.ts__day AS ts__day
    , subq_0.ts__week AS ts__week
    , subq_0.ts__month AS ts__month
    , subq_0.ts__quarter AS ts__quarter
    , subq_0.ts__year AS ts__year
    , subq_0.ts__extract_year AS ts__extract_year
    , subq_0.ts__extract_quarter AS ts__extract_quarter
    , subq_0.ts__extract_month AS ts__extract_month
    , subq_0.ts__extract_day AS ts__extract_day
    , subq_0.ts__extract_dow AS ts__extract_dow
    , subq_0.ts__extract_doy AS ts__extract_doy
    , subq_0.ts__hour AS metric_time__hour
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
    , subq_1.martian_day AS metric_time__martian_day
  FROM (
    -- Time Spine
    SELECT
      DATE_TRUNC('hour', time_spine_src_28005.ts) AS ts__hour
      , DATE_TRUNC('day', time_spine_src_28005.ts) AS ts__day
      , DATE_TRUNC('week', time_spine_src_28005.ts) AS ts__week
      , DATE_TRUNC('month', time_spine_src_28005.ts) AS ts__month
      , DATE_TRUNC('quarter', time_spine_src_28005.ts) AS ts__quarter
      , DATE_TRUNC('year', time_spine_src_28005.ts) AS ts__year
      , EXTRACT(year FROM time_spine_src_28005.ts) AS ts__extract_year
      , EXTRACT(quarter FROM time_spine_src_28005.ts) AS ts__extract_quarter
      , EXTRACT(month FROM time_spine_src_28005.ts) AS ts__extract_month
      , EXTRACT(day FROM time_spine_src_28005.ts) AS ts__extract_day
      , EXTRACT(isodow FROM time_spine_src_28005.ts) AS ts__extract_dow
      , EXTRACT(doy FROM time_spine_src_28005.ts) AS ts__extract_doy
    FROM ***************************.mf_time_spine_hour time_spine_src_28005
  ) subq_0
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_1
  ON
    subq_0.metric_time__day = subq_1.ds
) subq_2
GROUP BY
  subq_2.metric_time__martian_day
