test_name: test_window_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative window metric queried with non-default grains.

      Uses both metric_time and agg_time_dimension. Excludes default grain.
sql_engine: Trino
---
-- Re-aggregate Metric via Group By
SELECT
  booking__ds__month
  , metric_time__week
  , every_two_days_bookers_fill_nulls_with_0
FROM (
  -- Compute Metrics via Expressions
  -- Window Function for Metric Re-aggregation
  SELECT
    booking__ds__month
    , metric_time__week
    , FIRST_VALUE(COALESCE(bookers, 0)) OVER (
      PARTITION BY
        booking__ds__month
        , metric_time__week
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS every_two_days_bookers_fill_nulls_with_0
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      DATE_TRUNC('month', subq_20.ds) AS booking__ds__month
      , subq_20.ds AS metric_time__day
      , DATE_TRUNC('week', subq_20.ds) AS metric_time__week
      , subq_18.bookers AS bookers
    FROM ***************************.mf_time_spine subq_20
    LEFT OUTER JOIN (
      -- Join Self Over Time Range
      -- Pass Only Elements: ['bookers', 'metric_time__week', 'booking__ds__month', 'metric_time__day']
      -- Aggregate Measures
      SELECT
        DATE_TRUNC('month', subq_15.ds) AS booking__ds__month
        , subq_15.ds AS metric_time__day
        , DATE_TRUNC('week', subq_15.ds) AS metric_time__week
        , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
      FROM ***************************.mf_time_spine subq_15
      INNER JOIN
        ***************************.fct_bookings bookings_source_src_28000
      ON
        (
          DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_15.ds
        ) AND (
          DATE_TRUNC('day', bookings_source_src_28000.ds) > DATE_ADD('day', -2, subq_15.ds)
        )
      GROUP BY
        DATE_TRUNC('month', subq_15.ds)
        , subq_15.ds
        , DATE_TRUNC('week', subq_15.ds)
    ) subq_18
    ON
      subq_20.ds = subq_18.metric_time__day
  ) subq_21
) subq_23
GROUP BY
  booking__ds__month
  , metric_time__week
  , every_two_days_bookers_fill_nulls_with_0
