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
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , is_instant AS booking__is_instant
    , booking_value AS __average_booking_value
    , booking_value AS __max_booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , CAST(average_booking_value AS Nullable(Float64)) / CAST(NULLIF(max_booking_value, 0) AS Nullable(Float64)) AS instant_booking_fraction_of_max_value
FROM (
  SELECT
    COALESCE(subq_20.metric_time__day, subq_25.metric_time__day) AS metric_time__day
    , MAX(subq_20.average_booking_value) AS average_booking_value
    , MAX(subq_25.max_booking_value) AS max_booking_value
  FROM (
    SELECT
      metric_time__day
      , AVG(__average_booking_value) AS average_booking_value
    FROM (
      SELECT
        metric_time__day
        , average_booking_value AS __average_booking_value
      FROM (
        SELECT
          metric_time__day
          , booking__is_instant
          , __average_booking_value AS average_booking_value
        FROM sma_28009_cte
      ) subq_16
      WHERE booking__is_instant
    ) subq_18
    GROUP BY
      metric_time__day
  ) subq_20
  FULL OUTER JOIN (
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
