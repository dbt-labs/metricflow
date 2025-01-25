test_name: test_simple_metric_with_custom_granularity_filter
test_filename: test_custom_granularity.py
docstring:
  Simple metric queried with a filter on a custom grain, where that grain is not used in the group by.
sql_engine: Redshift
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(bookings) AS bookings
FROM (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  SELECT
    nr_subq_28002.bookings AS bookings
    , nr_subq_5.martian_day AS metric_time__martian_day
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS bookings
      , DATE_TRUNC('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_28002
  LEFT OUTER JOIN
    ***************************.mf_time_spine nr_subq_5
  ON
    nr_subq_28002.ds__day = nr_subq_5.ds
) nr_subq_6
WHERE metric_time__martian_day = '2020-01-01'
