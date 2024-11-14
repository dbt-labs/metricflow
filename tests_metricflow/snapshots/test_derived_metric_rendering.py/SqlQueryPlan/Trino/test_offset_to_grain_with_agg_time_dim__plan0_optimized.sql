test_name: test_offset_to_grain_with_agg_time_dim
test_filename: test_derived_metric_rendering.py
sql_engine: Trino
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    booking__ds__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'booking__ds__day']
    SELECT
      DATE_TRUNC('day', ds) AS booking__ds__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_16
  GROUP BY
    booking__ds__day
)

, cm_7_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'booking__ds__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_22.ds AS booking__ds__day
    , SUM(subq_20.bookings) AS bookings_at_start_of_month
  FROM ***************************.mf_time_spine subq_22
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS booking__ds__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_20
  ON
    DATE_TRUNC('month', subq_22.ds) = subq_20.booking__ds__day
  GROUP BY
    subq_22.ds
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    booking__ds__day
    , bookings - bookings_at_start_of_month AS bookings_growth_since_start_of_month
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.booking__ds__day, cm_7_cte.booking__ds__day) AS booking__ds__day
      , MAX(cm_6_cte.bookings) AS bookings
      , MAX(cm_7_cte.bookings_at_start_of_month) AS bookings_at_start_of_month
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
  , bookings_growth_since_start_of_month AS bookings_growth_since_start_of_month
FROM cm_8_cte cm_8_cte
