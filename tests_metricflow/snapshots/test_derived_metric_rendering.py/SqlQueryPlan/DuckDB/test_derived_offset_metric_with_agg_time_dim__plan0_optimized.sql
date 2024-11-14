test_name: test_derived_offset_metric_with_agg_time_dim
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['booking_value', 'booking__ds__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_17.ds AS booking__ds__day
    , SUM(bookings_source_src_28000.booking_value) AS booking_value
  FROM ***************************.mf_time_spine subq_17
  INNER JOIN
    ***************************.fct_bookings bookings_source_src_28000
  ON
    subq_17.ds - INTERVAL 1 week = DATE_TRUNC('day', bookings_source_src_28000.ds)
  GROUP BY
    subq_17.ds
)

, cm_7_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookers', 'booking__ds__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC('day', ds) AS booking__ds__day
    , COUNT(DISTINCT guest_id) AS bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('day', ds)
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    booking__ds__day
    , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.booking__ds__day, cm_7_cte.booking__ds__day) AS booking__ds__day
      , MAX(cm_6_cte.booking_value) AS booking_value
      , MAX(cm_7_cte.bookers) AS bookers
    FROM cm_6_cte cm_6_cte
    FULL OUTER JOIN
      cm_7_cte cm_7_cte
    ON
      cm_6_cte.booking__ds__day = cm_7_cte.booking__ds__day
    GROUP BY
      COALESCE(cm_6_cte.booking__ds__day, cm_7_cte.booking__ds__day)
  ) subq_27
)

SELECT
  booking__ds__day AS booking__ds__day
  , booking_fees_last_week_per_booker_this_week AS booking_fees_last_week_per_booker_this_week
FROM cm_8_cte cm_8_cte
