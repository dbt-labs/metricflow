test_name: test_simple_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Postgres
---
-- Metric Time Dimension 'ds'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['bookings', 'booking__ds__martian_day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  nr_subq_4.martian_day AS booking__ds__martian_day
  , SUM(nr_subq_28002.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  SELECT
    1 AS bookings
    , DATE_TRUNC('day', ds) AS booking__ds__day
  FROM ***************************.fct_bookings bookings_source_src_28000
) nr_subq_28002
LEFT OUTER JOIN
  ***************************.mf_time_spine nr_subq_4
ON
  nr_subq_28002.booking__ds__day = nr_subq_4.ds
GROUP BY
  nr_subq_4.martian_day
