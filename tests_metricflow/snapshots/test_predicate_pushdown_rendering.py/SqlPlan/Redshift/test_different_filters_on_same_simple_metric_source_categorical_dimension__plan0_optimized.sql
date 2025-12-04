test_name: test_different_filters_on_same_simple_metric_source_categorical_dimension
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where multiple filters against the same simple-metric input dimension need to be an effective OR.

      This can be an issue where a derived metric takes in two filters that refer to the same dimension from the input
      measure source. If these filters are disjoint the predicate pushdown needs to ensure that all matching rows are
      returned, so we cannot simply push one filter or the other down, nor can we push them down as an AND - they
      must be an OR, since all relevant rows need to be returned to the requesting metrics.

      The metric listed here has one input that filters on bookings__is_instant and another that does not, which means
      the source input for the latter input must NOT have the filter applied to it.
sql_engine: Redshift
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , is_instant AS booking__is_instant
    , booking_value AS __average_booking_value
    , booking_value AS __max_booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , CAST(average_booking_value AS DOUBLE PRECISION) / CAST(NULLIF(max_booking_value, 0) AS DOUBLE PRECISION) AS instant_booking_fraction_of_max_value
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_20.metric_time__day, subq_25.metric_time__day) AS metric_time__day
    , MAX(subq_20.average_booking_value) AS average_booking_value
    , MAX(subq_25.max_booking_value) AS max_booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__average_booking_value', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , AVG(average_booking_value) AS average_booking_value
    FROM (
      -- Read From CTE For node_id=sma_28009
      -- Pass Only Elements: ['__average_booking_value', 'booking__is_instant', 'metric_time__day']
      SELECT
        metric_time__day
        , booking__is_instant
        , __average_booking_value AS average_booking_value
      FROM sma_28009_cte
    ) subq_16
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
  ) subq_20
  FULL OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['__max_booking_value', 'metric_time__day']
    -- Pass Only Elements: ['__max_booking_value', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , MAX(__max_booking_value) AS max_booking_value
    FROM sma_28009_cte
    GROUP BY
      metric_time__day
  ) subq_25
  ON
    subq_20.metric_time__day = subq_25.metric_time__day
  GROUP BY
    COALESCE(subq_20.metric_time__day, subq_25.metric_time__day)
) subq_26
