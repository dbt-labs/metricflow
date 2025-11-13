test_name: test_offset_window_with_agg_time_dim
test_filename: test_derived_metric_rendering.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS booking__ds__day
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  booking__ds__day AS booking__ds__day
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_19.booking__ds__day, subq_27.booking__ds__day) AS booking__ds__day
    , MAX(subq_19.bookings) AS bookings
    , MAX(subq_27.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['__bookings', 'booking__ds__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      booking__ds__day
      , SUM(__bookings) AS bookings
    FROM sma_28009_cte
    GROUP BY
      booking__ds__day
  ) subq_19
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS booking__ds__day
      , subq_22.__bookings AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read From CTE For node_id=sma_28009
      -- Pass Only Elements: ['__bookings', 'booking__ds__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        booking__ds__day
        , SUM(__bookings) AS __bookings
      FROM sma_28009_cte
      GROUP BY
        booking__ds__day
    ) subq_22
    ON
      DATEADD(day, -14, time_spine_src_28006.ds) = subq_22.booking__ds__day
  ) subq_27
  ON
    subq_19.booking__ds__day = subq_27.booking__ds__day
  GROUP BY
    COALESCE(subq_19.booking__ds__day, subq_27.booking__ds__day)
) subq_28
