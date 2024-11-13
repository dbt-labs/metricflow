test_name: test_offset_window_with_agg_time_dim
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
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
      DATETIME_TRUNC(ds, day) AS booking__ds__day
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
    , SUM(subq_20.bookings) AS bookings_2_weeks_ago
  FROM ***************************.mf_time_spine subq_22
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS booking__ds__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_20
  ON
    DATE_SUB(CAST(subq_22.ds AS DATETIME), INTERVAL 14 day) = subq_20.booking__ds__day
  GROUP BY
    booking__ds__day
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    booking__ds__day
    , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.booking__ds__day, cm_7_cte.booking__ds__day) AS booking__ds__day
      , MAX(cm_6_cte.bookings) AS bookings
      , MAX(cm_7_cte.bookings_2_weeks_ago) AS bookings_2_weeks_ago
    FROM cm_6_cte cm_6_cte
    FULL OUTER JOIN
      cm_7_cte cm_7_cte
    ON
      cm_6_cte.booking__ds__day = cm_7_cte.booking__ds__day
    GROUP BY
      booking__ds__day
  ) subq_27
)

SELECT
  booking__ds__day AS booking__ds__day
  , bookings_growth_2_weeks AS bookings_growth_2_weeks
FROM cm_8_cte cm_8_cte
