-- Pass Only Elements:
--   ['metric_time__day']
SELECT
  subq_1.metric_time__day
FROM (
  -- Metric Time Dimension 'ds'
  SELECT
    subq_0.ds__day
    , subq_0.ds__week
    , subq_0.ds__month
    , subq_0.ds__quarter
    , subq_0.ds__year
    , subq_0.ds__day AS metric_time__day
    , subq_0.ds__week AS metric_time__week
    , subq_0.ds__month AS metric_time__month
    , subq_0.ds__quarter AS metric_time__quarter
    , subq_0.ds__year AS metric_time__year
  FROM (
    -- Date Spine
    SELECT
      DATE_TRUNC('day', time_spine_src_0.ds) AS ds__day
      , DATE_TRUNC('week', time_spine_src_0.ds) AS ds__week
      , DATE_TRUNC('month', time_spine_src_0.ds) AS ds__month
      , DATE_TRUNC('quarter', time_spine_src_0.ds) AS ds__quarter
      , DATE_TRUNC('year', time_spine_src_0.ds) AS ds__year
    FROM ***************************.mf_time_spine time_spine_src_0
  ) subq_0
) subq_1
GROUP BY
  subq_1.metric_time__day
