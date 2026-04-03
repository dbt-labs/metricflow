test_name: test_simple_metric_constraint_with_reused_simple_metric
test_filename: test_query_rendering.py
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , is_instant AS booking__is_instant
    , booking_value AS __booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , CAST(booking_value_with_is_instant_constraint AS Nullable(Float64)) / CAST(NULLIF(booking_value, 0) AS Nullable(Float64)) AS instant_booking_value_ratio
FROM (
  SELECT
    COALESCE(subq_20.metric_time__day, subq_25.metric_time__day) AS metric_time__day
    , MAX(subq_20.booking_value_with_is_instant_constraint) AS booking_value_with_is_instant_constraint
    , MAX(subq_25.booking_value) AS booking_value
  FROM (
    SELECT
      metric_time__day
      , SUM(__booking_value) AS booking_value_with_is_instant_constraint
    FROM (
      SELECT
        metric_time__day
        , booking_value AS __booking_value
      FROM (
        SELECT
          metric_time__day
          , booking__is_instant
          , __booking_value AS booking_value
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
      , SUM(__booking_value) AS booking_value
    FROM sma_28009_cte
    GROUP BY
      metric_time__day
  ) subq_25
  ON
    subq_20.metric_time__day = subq_25.metric_time__day
  GROUP BY
    COALESCE(subq_20.metric_time__day, subq_25.metric_time__day)
) subq_26
