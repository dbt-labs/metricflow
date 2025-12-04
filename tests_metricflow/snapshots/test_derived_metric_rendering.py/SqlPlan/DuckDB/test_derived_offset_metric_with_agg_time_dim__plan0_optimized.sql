test_name: test_derived_offset_metric_with_agg_time_dim
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS booking__ds__day
    , booking_value AS __booking_value
    , guest_id AS __bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  booking__ds__day AS booking__ds__day
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_28.booking__ds__day, subq_33.booking__ds__day) AS booking__ds__day
    , MAX(subq_28.booking_value) AS booking_value
    , MAX(subq_33.bookers) AS bookers
  FROM (
    -- Join to Time Spine Dataset
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS booking__ds__day
      , subq_22.__booking_value AS booking_value
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read From CTE For node_id=sma_28009
      -- Pass Only Elements: ['__booking_value', 'booking__ds__day']
      -- Pass Only Elements: ['__booking_value', 'booking__ds__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        booking__ds__day
        , SUM(__booking_value) AS __booking_value
      FROM sma_28009_cte
      GROUP BY
        booking__ds__day
    ) subq_22
    ON
      time_spine_src_28006.ds - INTERVAL 1 week = subq_22.booking__ds__day
  ) subq_28
  FULL OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['__bookers', 'booking__ds__day']
    -- Pass Only Elements: ['__bookers', 'booking__ds__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      booking__ds__day
      , COUNT(DISTINCT __bookers) AS bookers
    FROM sma_28009_cte
    GROUP BY
      booking__ds__day
  ) subq_33
  ON
    subq_28.booking__ds__day = subq_33.booking__ds__day
  GROUP BY
    COALESCE(subq_28.booking__ds__day, subq_33.booking__ds__day)
) subq_34
