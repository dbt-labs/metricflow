-- Re-aggregate Metric via Group By
SELECT
  metric_time__week
  , booking__ds__month
  , every_two_days_bookers_fill_nulls_with_0
FROM (
  -- Compute Metrics via Expressions
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__week
    , booking__ds__month
    , FIRST_VALUE(COALESCE(bookers, 0)) OVER (
      PARTITION BY
        metric_time__week
        , booking__ds__month
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS every_two_days_bookers_fill_nulls_with_0
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_18.ds AS metric_time__day
      , DATE_TRUNC('week', subq_18.ds) AS metric_time__week
      , subq_16.booking__ds__month AS booking__ds__month
      , subq_16.bookers AS bookers
    FROM ***************************.mf_time_spine subq_18
    LEFT OUTER JOIN (
      -- Join Self Over Time Range
      -- Pass Only Elements: ['bookers', 'metric_time__week', 'booking__ds__month', 'metric_time__day']
      -- Aggregate Measures
      SELECT
        subq_13.booking__ds__month AS booking__ds__month
        , subq_13.metric_time__day AS metric_time__day
        , subq_13.metric_time__week AS metric_time__week
        , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
      FROM (
        -- Time Spine
        SELECT
          DATE_TRUNC('month', ds) AS booking__ds__month
          , ds AS metric_time__day
          , DATE_TRUNC('week', ds) AS metric_time__week
        FROM ***************************.mf_time_spine subq_14
        GROUP BY
          DATE_TRUNC('month', ds)
          , ds
          , DATE_TRUNC('week', ds)
      ) subq_13
      INNER JOIN
        ***************************.fct_bookings bookings_source_src_28000
      ON
        (
          DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_13.metric_time__day
        ) AND (
          DATE_TRUNC('day', bookings_source_src_28000.ds) > subq_13.metric_time__day - INTERVAL 2 day
        )
      GROUP BY
        subq_13.booking__ds__month
        , subq_13.metric_time__day
        , subq_13.metric_time__week
    ) subq_16
    ON
      subq_18.ds = subq_16.metric_time__day
  ) subq_19
) subq_21
GROUP BY
  metric_time__week
  , booking__ds__month
  , every_two_days_bookers_fill_nulls_with_0
