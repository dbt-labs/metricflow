-- Re-aggregate Metrics via Window Functions
SELECT
  metric_time__week
  , booking__ds__month
  , every_two_days_bookers_fill_nulls_with_0
FROM (
  -- Compute Metrics via Expressions
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
      subq_20.ds AS metric_time__day
      , DATE_TRUNC('week', subq_20.ds) AS metric_time__week
      , subq_18.booking__ds__month AS booking__ds__month
      , subq_18.bookers AS bookers
    FROM ***************************.mf_time_spine subq_20
    LEFT OUTER JOIN (
      -- Join Self Over Time Range
      -- Pass Only Elements: ['bookers', 'metric_time__week', 'booking__ds__month', 'metric_time__day']
      -- Aggregate Measures
      SELECT
        subq_14.booking__ds__month AS booking__ds__month
        , subq_14.metric_time__day AS metric_time__day
        , subq_14.metric_time__week AS metric_time__week
        , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
      FROM (
        -- Time Spine
        SELECT
          DATE_TRUNC('month', ds) AS booking__ds__month
          , ds AS metric_time__day
          , DATE_TRUNC('week', ds) AS metric_time__week
        FROM ***************************.mf_time_spine subq_15
        GROUP BY
          DATE_TRUNC('month', ds)
          , ds
          , DATE_TRUNC('week', ds)
      ) subq_14
      INNER JOIN
        ***************************.fct_bookings bookings_source_src_28000
      ON
        (
          DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_14.metric_time__day
        ) AND (
          DATE_TRUNC('day', bookings_source_src_28000.ds) > subq_14.metric_time__day - INTERVAL 2 day
        )
      GROUP BY
        subq_14.booking__ds__month
        , subq_14.metric_time__day
        , subq_14.metric_time__week
    ) subq_18
    ON
      subq_20.ds = subq_18.metric_time__day
  ) subq_21
) subq_23
GROUP BY
  metric_time__week
  , booking__ds__month
  , every_two_days_bookers_fill_nulls_with_0
