test_name: test_derived_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Postgres
---
-- Read From CTE For node_id=cm_9
WITH cm_8_cte AS (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['booking_value', 'bookers', 'booking__ds__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_14.martian_day AS booking__ds__martian_day
    , SUM(bookings_source_src_28000.booking_value) AS booking_value
    , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_14
  ON
    DATE_TRUNC('day', bookings_source_src_28000.ds) = subq_14.ds
  GROUP BY
    subq_14.martian_day
)

, cm_9_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    booking__ds__martian_day
    , booking_value * 0.05 / bookers AS booking_fees_per_booker
  FROM (
    -- Read From CTE For node_id=cm_8
    SELECT
      booking__ds__martian_day
      , booking_value
      , bookers
    FROM cm_8_cte cm_8_cte
  ) subq_18
)

SELECT
  booking__ds__martian_day AS booking__ds__martian_day
  , booking_fees_per_booker AS booking_fees_per_booker
FROM cm_9_cte cm_9_cte
