test_name: test_nested_derived_metric_with_offset_multiple_input_metrics
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , booking_value AS __booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , booking_fees - booking_fees_start_of_month AS booking_fees_since_start_of_month
FROM (
  SELECT
    COALESCE(subq_31.metric_time__day, subq_37.metric_time__day) AS metric_time__day
    , MAX(subq_31.booking_fees_start_of_month) AS booking_fees_start_of_month
    , MAX(subq_37.booking_fees) AS booking_fees
  FROM (
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , subq_26.booking_fees_start_of_month AS booking_fees_start_of_month
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      SELECT
        metric_time__day
        , booking_value * 0.05 AS booking_fees_start_of_month
      FROM (
        SELECT
          metric_time__day
          , SUM(__booking_value) AS booking_value
        FROM sma_28009_cte
        GROUP BY
          metric_time__day
      ) subq_25
    ) subq_26
    ON
      toStartOfMonth(time_spine_src_28006.ds) = subq_26.metric_time__day
  ) subq_31
  FULL OUTER JOIN (
    SELECT
      metric_time__day
      , booking_value * 0.05 AS booking_fees
    FROM (
      SELECT
        metric_time__day
        , SUM(__booking_value) AS booking_value
      FROM sma_28009_cte
      GROUP BY
        metric_time__day
    ) subq_36
  ) subq_37
  ON
    subq_31.metric_time__day = subq_37.metric_time__day
  GROUP BY
    COALESCE(subq_31.metric_time__day, subq_37.metric_time__day)
) subq_38
