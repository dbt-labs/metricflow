test_name: test_derived_offset_metric_with_agg_time_dim
test_filename: test_derived_metric_rendering.py
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS booking__ds__day
    , booking_value
    , guest_id AS bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  booking__ds__day AS booking__ds__day
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_23.booking__ds__day, subq_27.booking__ds__day) AS booking__ds__day
    , MAX(subq_23.booking_value) AS booking_value
    , MAX(subq_27.bookers) AS bookers
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['booking_value', 'booking__ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS booking__ds__day
      , SUM(sma_28009_cte.booking_value) AS booking_value
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN
      sma_28009_cte
    ON
      DATEADD(week, -1, time_spine_src_28006.ds) = sma_28009_cte.booking__ds__day
    GROUP BY
      time_spine_src_28006.ds
  ) subq_23
  FULL OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['bookers', 'booking__ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      booking__ds__day
      , COUNT(DISTINCT bookers) AS bookers
    FROM sma_28009_cte
    GROUP BY
      booking__ds__day
  ) subq_27
  ON
    subq_23.booking__ds__day = subq_27.booking__ds__day
  GROUP BY
    COALESCE(subq_23.booking__ds__day, subq_27.booking__ds__day)
) subq_28
