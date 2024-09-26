-- Pass Only Elements: ['metric_time__martian_day',]
SELECT
  subq_2.metric_time__martian_day
FROM (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  SELECT
    subq_0.ds__day AS ds__day
    , subq_0.ds__week AS ds__week
    , subq_0.ds__month AS ds__month
    , subq_0.ds__quarter AS ds__quarter
    , subq_0.ds__year AS ds__year
    , subq_0.ds__extract_year AS ds__extract_year
    , subq_0.ds__extract_quarter AS ds__extract_quarter
    , subq_0.ds__extract_month AS ds__extract_month
    , subq_0.ds__extract_day AS ds__extract_day
    , subq_0.ds__extract_dow AS ds__extract_dow
    , subq_0.ds__extract_doy AS ds__extract_doy
    , subq_0.ds__martian_day AS ds__martian_day
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
    , subq_1.martian_day AS metric_time__martian_day
  FROM (
    -- Time Spine
    SELECT
      DATE_TRUNC('day', time_spine_src_28006.ds) AS ds__day
      , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
      , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
      , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
      , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
      , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
      , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
      , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
      , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
      , CASE WHEN EXTRACT(dow FROM time_spine_src_28006.ds) = 0 THEN EXTRACT(dow FROM time_spine_src_28006.ds) + 7 ELSE EXTRACT(dow FROM time_spine_src_28006.ds) END AS ds__extract_dow
      , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
      , time_spine_src_28006.martian_day AS ds__martian_day
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) subq_0
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_1
  ON
    subq_0.ds__day = subq_1.ds
) subq_2
GROUP BY
  subq_2.metric_time__martian_day
