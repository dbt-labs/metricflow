-- Metric Time Dimension 'ts'
-- Pass Only Elements: ['metric_time__millisecond',]
SELECT
  subq_0.ts__millisecond AS metric_time__millisecond
FROM (
  -- Time Spine
  SELECT
    DATE_TRUNC('millisecond', time_spine_src_28002.ts) AS ts__millisecond
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
) subq_0
GROUP BY
  subq_0.ts__millisecond
