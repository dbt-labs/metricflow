test_name: test_cumulative_metric_with_non_adjustable_time_filter
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a cumulative metric query with a time filter that cannot be automatically adjusted.

      Not all query inputs with time constraint filters allow us to adjust the time constraint to include the full
      span of input data for a cumulative metric. When we do not have an adjustable time filter we must include all
      input data in order to ensure the cumulative metric is correct.
sql_engine: Snowflake
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__bookers', 'metric_time__day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , COUNT(DISTINCT bookers) AS every_two_days_bookers
FROM (
  -- Join Self Over Time Range
  -- Pass Only Elements: ['__bookers', 'metric_time__day']
  SELECT
    subq_14.ds AS metric_time__day
    , bookings_source_src_28000.guest_id AS bookers
  FROM ***************************.mf_time_spine subq_14
  INNER JOIN
    ***************************.fct_bookings bookings_source_src_28000
  ON
    (
      DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_14.ds
    ) AND (
      DATE_TRUNC('day', bookings_source_src_28000.ds) > DATEADD(day, -2, subq_14.ds)
    )
) subq_16
WHERE metric_time__day = '2020-01-03' or metric_time__day = '2020-01-07'
GROUP BY
  metric_time__day
