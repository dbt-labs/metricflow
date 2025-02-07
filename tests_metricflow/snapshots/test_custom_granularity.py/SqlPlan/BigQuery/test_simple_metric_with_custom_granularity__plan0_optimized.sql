test_name: test_simple_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: BigQuery
---
-- Metric Time Dimension 'ds'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['bookings', 'booking__ds__alien_day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_6.alien_day AS booking__ds__alien_day
  , SUM(subq_5.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  SELECT
    1 AS bookings
    , DATETIME_TRUNC(ds, day) AS booking__ds__day
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_5
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_6
ON
  subq_5.booking__ds__day = subq_6.ds
GROUP BY
  booking__ds__alien_day
