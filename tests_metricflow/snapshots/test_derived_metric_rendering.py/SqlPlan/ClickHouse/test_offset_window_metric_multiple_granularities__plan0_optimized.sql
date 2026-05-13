test_name: test_offset_window_metric_multiple_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset window metric is queried with multiple granularities.
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , toStartOfMonth(ds) AS metric_time__month
    , toStartOfYear(ds) AS metric_time__year
    , booking_value AS __booking_value
    , guest_id AS __bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , metric_time__month AS metric_time__month
  , metric_time__year AS metric_time__year
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  SELECT
    COALESCE(subq_28.metric_time__day, subq_33.metric_time__day) AS metric_time__day
    , COALESCE(subq_28.metric_time__month, subq_33.metric_time__month) AS metric_time__month
    , COALESCE(subq_28.metric_time__year, subq_33.metric_time__year) AS metric_time__year
    , MAX(subq_28.booking_value) AS booking_value
    , MAX(subq_33.bookers) AS bookers
  FROM (
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , toStartOfMonth(time_spine_src_28006.ds) AS metric_time__month
      , toStartOfYear(time_spine_src_28006.ds) AS metric_time__year
      , subq_22.__booking_value AS booking_value
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      SELECT
        metric_time__day
        , metric_time__month
        , metric_time__year
        , SUM(__booking_value) AS __booking_value
      FROM sma_28009_cte
      GROUP BY
        metric_time__day
        , metric_time__month
        , metric_time__year
    ) subq_22
    ON
      addDays(time_spine_src_28006.ds, -7) = subq_22.metric_time__day
  ) subq_28
  FULL OUTER JOIN (
    SELECT
      metric_time__day
      , metric_time__month
      , metric_time__year
      , COUNT(DISTINCT __bookers) AS bookers
    FROM sma_28009_cte
    GROUP BY
      metric_time__day
      , metric_time__month
      , metric_time__year
  ) subq_33
  ON
    (
      subq_28.metric_time__day = subq_33.metric_time__day
    ) AND (
      subq_28.metric_time__month = subq_33.metric_time__month
    ) AND (
      subq_28.metric_time__year = subq_33.metric_time__year
    )
  GROUP BY
    COALESCE(subq_28.metric_time__day, subq_33.metric_time__day)
    , COALESCE(subq_28.metric_time__month, subq_33.metric_time__month)
    , COALESCE(subq_28.metric_time__year, subq_33.metric_time__year)
) subq_34
