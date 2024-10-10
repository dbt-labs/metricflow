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
      subq_20.ds AS metric_time__day
      , DATETIME_TRUNC(subq_20.ds, isoweek) AS metric_time__week
      , subq_18.booking__ds__month AS booking__ds__month
      , subq_18.bookers AS bookers
    FROM ***************************.mf_time_spine subq_20
    LEFT OUTER JOIN (
      -- Join Self Over Time Range
      -- Pass Only Elements: ['bookers', 'metric_time__week', 'booking__ds__month', 'metric_time__day']
      -- Aggregate Measures
      SELECT
        DATETIME_TRUNC(subq_15.ds, month) AS booking__ds__month
        , subq_15.ds AS metric_time__day
        , DATETIME_TRUNC(subq_15.ds, isoweek) AS metric_time__week
        , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
      FROM ***************************.mf_time_spine subq_15
      INNER JOIN
        ***************************.fct_bookings bookings_source_src_28000
      ON
        (
          DATETIME_TRUNC(bookings_source_src_28000.ds, day) <= subq_15.ds
        ) AND (
          DATETIME_TRUNC(bookings_source_src_28000.ds, day) > DATE_SUB(CAST(subq_15.ds AS DATETIME), INTERVAL 2 day)
        )
      GROUP BY
        booking__ds__month
        , metric_time__day
        , metric_time__week
    ) subq_18
    ON
      subq_20.ds = subq_18.metric_time__day
  ) subq_21
) subq_23
GROUP BY
  metric_time__week
  , booking__ds__month
  , every_two_days_bookers_fill_nulls_with_0
