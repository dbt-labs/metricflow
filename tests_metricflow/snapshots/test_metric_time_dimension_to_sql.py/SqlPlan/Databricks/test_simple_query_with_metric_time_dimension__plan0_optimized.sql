test_name: test_simple_query_with_metric_time_dimension
test_filename: test_metric_time_dimension_to_sql.py
docstring:
  Tests building a query that uses simple-metric inputs defined from 2 different time dimensions.
sql_engine: Databricks
---
-- Combine Aggregated Outputs
-- Write to DataTable
WITH rss_28020_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  SELECT
    1 AS __bookings
    , booking_value AS __booking_payments
    , DATE_TRUNC('day', ds) AS ds__day
    , DATE_TRUNC('day', paid_at) AS paid_at__day
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  COALESCE(subq_18.metric_time__day, subq_24.metric_time__day) AS metric_time__day
  , MAX(subq_18.bookings) AS bookings
  , MAX(subq_24.booking_payments) AS booking_payments
FROM (
  -- Read From CTE For node_id=rss_28020
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['__bookings', 'metric_time__day']
  -- Pass Only Elements: ['__bookings', 'metric_time__day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    ds__day AS metric_time__day
    , SUM(__bookings) AS bookings
  FROM rss_28020_cte
  GROUP BY
    ds__day
) subq_18
FULL OUTER JOIN (
  -- Read From CTE For node_id=rss_28020
  -- Metric Time Dimension 'paid_at'
  -- Pass Only Elements: ['__booking_payments', 'metric_time__day']
  -- Pass Only Elements: ['__booking_payments', 'metric_time__day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    paid_at__day AS metric_time__day
    , SUM(__booking_payments) AS booking_payments
  FROM rss_28020_cte
  GROUP BY
    paid_at__day
) subq_24
ON
  subq_18.metric_time__day = subq_24.metric_time__day
GROUP BY
  COALESCE(subq_18.metric_time__day, subq_24.metric_time__day)
