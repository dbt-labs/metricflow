test_name: test_simple_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Snowflake
---
-- Metric Time Dimension 'ds'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['__bookings', 'booking__ds__alien_day']
-- Pass Only Elements: ['__bookings', 'booking__ds__alien_day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_8.alien_day AS booking__ds__alien_day
  , SUM(subq_7.__bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  SELECT
    1 AS __bookings
    , DATE_TRUNC('day', ds) AS booking__ds__day
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_7
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_8
ON
  subq_7.booking__ds__day = subq_8.ds
GROUP BY
  subq_8.alien_day
