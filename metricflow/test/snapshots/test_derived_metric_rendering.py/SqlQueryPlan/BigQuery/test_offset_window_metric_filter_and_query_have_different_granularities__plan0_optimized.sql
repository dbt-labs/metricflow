-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_27.metric_time__month, subq_34.metric_time__month) AS metric_time__month
    , MAX(subq_27.booking_value) AS booking_value
    , MAX(subq_34.bookers) AS bookers
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['booking_value', 'metric_time__month']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__month
      , SUM(booking_value) AS booking_value
    FROM (
      -- Join to Time Spine Dataset
      -- Pass Only Elements: ['booking_value', 'metric_time__month', 'metric_time__day']
      SELECT
        subq_21.ds AS metric_time__day
        , DATE_TRUNC(subq_21.ds, month) AS metric_time__month
        , bookings_source_src_28000.booking_value AS booking_value
      FROM ***************************.mf_time_spine subq_21
      INNER JOIN
        ***************************.fct_bookings bookings_source_src_28000
      ON
        DATE_SUB(CAST(subq_21.ds AS DATETIME), INTERVAL 1 week) = DATE_TRUNC(bookings_source_src_28000.ds, day)
    ) subq_23
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) subq_27
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookers', 'metric_time__month']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__month
      , COUNT(DISTINCT bookers) AS bookers
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookers', 'metric_time__month', 'metric_time__day']
      SELECT
        DATE_TRUNC(ds, day) AS metric_time__day
        , DATE_TRUNC(ds, month) AS metric_time__month
        , guest_id AS bookers
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_30
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) subq_34
  ON
    subq_27.metric_time__month = subq_34.metric_time__month
  GROUP BY
    metric_time__month
) subq_35
