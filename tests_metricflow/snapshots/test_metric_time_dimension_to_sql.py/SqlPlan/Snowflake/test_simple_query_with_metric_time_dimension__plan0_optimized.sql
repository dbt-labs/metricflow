test_name: test_simple_query_with_metric_time_dimension
test_filename: test_metric_time_dimension_to_sql.py
docstring:
  Tests building a query that uses measures defined from 2 different time dimensions.
sql_engine: Snowflake
---
-- Combine Aggregated Outputs
SELECT
  COALESCE(nr_subq_3.metric_time__day, nr_subq_7.metric_time__day) AS metric_time__day
  , MAX(nr_subq_3.bookings) AS bookings
  , MAX(nr_subq_7.booking_payments) AS booking_payments
FROM (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_1
  GROUP BY
    metric_time__day
) nr_subq_3
FULL OUTER JOIN (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'paid_at'
  -- Pass Only Elements: ['booking_payments', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC('day', paid_at) AS metric_time__day
    , SUM(booking_value) AS booking_payments
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('day', paid_at)
) nr_subq_7
ON
  nr_subq_3.metric_time__day = nr_subq_7.metric_time__day
GROUP BY
  COALESCE(nr_subq_3.metric_time__day, nr_subq_7.metric_time__day)
