-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , metric_time__month
  , metric_time__year
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_21.metric_time__day, subq_26.metric_time__day) AS metric_time__day
    , COALESCE(subq_21.metric_time__month, subq_26.metric_time__month) AS metric_time__month
    , COALESCE(subq_21.metric_time__year, subq_26.metric_time__year) AS metric_time__year
    , MAX(subq_21.booking_value) AS booking_value
    , MAX(subq_26.bookers) AS bookers
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['booking_value', 'metric_time__day', 'metric_time__month', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_17.ds AS metric_time__day
      , DATE_TRUNC('month', subq_17.ds) AS metric_time__month
      , DATE_TRUNC('year', subq_17.ds) AS metric_time__year
      , SUM(bookings_source_src_28000.booking_value) AS booking_value
    FROM ***************************.mf_time_spine subq_17
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      subq_17.ds - MAKE_INTERVAL(weeks => 1) = DATE_TRUNC('day', bookings_source_src_28000.ds)
    GROUP BY
      subq_17.ds
      , DATE_TRUNC('month', subq_17.ds)
      , DATE_TRUNC('year', subq_17.ds)
  ) subq_21
  FULL OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookers', 'metric_time__day', 'metric_time__month', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , DATE_TRUNC('month', ds) AS metric_time__month
      , DATE_TRUNC('year', ds) AS metric_time__year
      , COUNT(DISTINCT guest_id) AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      DATE_TRUNC('day', ds)
      , DATE_TRUNC('month', ds)
      , DATE_TRUNC('year', ds)
  ) subq_26
  ON
    (
      subq_21.metric_time__day = subq_26.metric_time__day
    ) AND (
      subq_21.metric_time__month = subq_26.metric_time__month
    ) AND (
      subq_21.metric_time__year = subq_26.metric_time__year
    )
  GROUP BY
    COALESCE(subq_21.metric_time__day, subq_26.metric_time__day)
    , COALESCE(subq_21.metric_time__month, subq_26.metric_time__month)
    , COALESCE(subq_21.metric_time__year, subq_26.metric_time__year)
) subq_27
