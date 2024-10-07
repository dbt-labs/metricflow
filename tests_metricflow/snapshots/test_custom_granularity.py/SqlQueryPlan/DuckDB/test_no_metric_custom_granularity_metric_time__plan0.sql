-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['metric_time__martian_day',]
SELECT
  subq_0.ds__martian_day AS metric_time__martian_day
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
    , EXTRACT(isodow FROM time_spine_src_28006.ds) AS ds__extract_dow
    , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
    , time_spine_src_28006.martian_day AS ds__martian_day
  FROM ***************************.mf_time_spine time_spine_src_28006
) subq_0
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_1
ON
  subq_0.ds__day = subq_1.ds
GROUP BY
  subq_0.ds__martian_day
  , subq_1.martian_day
