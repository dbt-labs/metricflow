test_name: test_nested_derived_metric
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , 1 AS __bookings
    , CASE WHEN is_instant THEN 1 ELSE 0 END AS __instant_bookings
    , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS __referred_bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , non_referred + (instant * 1.0 / bookings) AS instant_plus_non_referred_bookings_pct
FROM (
  SELECT
    COALESCE(subq_27.metric_time__day, subq_32.metric_time__day) AS metric_time__day
    , MAX(subq_27.non_referred) AS non_referred
    , MAX(subq_32.instant) AS instant
    , MAX(subq_32.bookings) AS bookings
  FROM (
    SELECT
      metric_time__day
      , (bookings - ref_bookings) * 1.0 / bookings AS non_referred
    FROM (
      SELECT
        metric_time__day
        , SUM(__referred_bookings) AS ref_bookings
        , SUM(__bookings) AS bookings
      FROM sma_28009_cte
      GROUP BY
        metric_time__day
    ) subq_26
  ) subq_27
  FULL OUTER JOIN (
    SELECT
      metric_time__day
      , SUM(__instant_bookings) AS instant
      , SUM(__bookings) AS bookings
    FROM sma_28009_cte
    GROUP BY
      metric_time__day
  ) subq_32
  ON
    subq_27.metric_time__day = subq_32.metric_time__day
  GROUP BY
    COALESCE(subq_27.metric_time__day, subq_32.metric_time__day)
) subq_33
