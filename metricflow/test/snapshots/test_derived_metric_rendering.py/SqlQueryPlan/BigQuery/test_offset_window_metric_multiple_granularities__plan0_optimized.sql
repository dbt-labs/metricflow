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
      , DATE_TRUNC(subq_17.ds, month) AS metric_time__month
      , DATE_TRUNC(subq_17.ds, year) AS metric_time__year
      , SUM(bookings_source_src_28000.booking_value) AS booking_value
    FROM ***************************.mf_time_spine subq_17
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      DATE_SUB(CAST(subq_17.ds AS DATETIME), INTERVAL 1 week) = DATE_TRUNC(bookings_source_src_28000.ds, day)
    GROUP BY
      metric_time__day
      , metric_time__month
      , metric_time__year
  ) subq_21
  FULL OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookers', 'metric_time__day', 'metric_time__month', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC(ds, day) AS metric_time__day
      , DATE_TRUNC(ds, month) AS metric_time__month
      , DATE_TRUNC(ds, year) AS metric_time__year
      , COUNT(DISTINCT guest_id) AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      metric_time__day
      , metric_time__month
      , metric_time__year
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
    metric_time__day
    , metric_time__month
    , metric_time__year
) subq_27
