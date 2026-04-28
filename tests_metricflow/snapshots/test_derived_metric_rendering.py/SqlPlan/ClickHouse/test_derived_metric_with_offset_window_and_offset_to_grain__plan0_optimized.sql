test_name: test_derived_metric_with_offset_window_and_offset_to_grain
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

, rss_28018_cte AS (
  SELECT
    ds AS ds__day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  SELECT
    COALESCE(subq_33.metric_time__day, subq_43.metric_time__day) AS metric_time__day
    , MAX(subq_33.month_start_bookings) AS month_start_bookings
    , MAX(subq_43.bookings_1_month_ago) AS bookings_1_month_ago
  FROM (
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_27.__bookings AS month_start_bookings
    FROM rss_28018_cte
    INNER JOIN (
      SELECT
        metric_time__day
        , SUM(__bookings) AS __bookings
      FROM sma_28009_cte
      GROUP BY
        metric_time__day
    ) subq_27
    ON
      toStartOfMonth(rss_28018_cte.ds__day) = subq_27.metric_time__day
  ) subq_33
  FULL OUTER JOIN (
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_37.__bookings AS bookings_1_month_ago
    FROM rss_28018_cte
    INNER JOIN (
      SELECT
        metric_time__day
        , SUM(__bookings) AS __bookings
      FROM sma_28009_cte
      GROUP BY
        metric_time__day
    ) subq_37
    ON
      addMonths(rss_28018_cte.ds__day, -1) = subq_37.metric_time__day
  ) subq_43
  ON
    subq_33.metric_time__day = subq_43.metric_time__day
  GROUP BY
    COALESCE(subq_33.metric_time__day, subq_43.metric_time__day)
) subq_44
