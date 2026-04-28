test_name: test_derived_metric_with_offset_window_and_offset_to_grain_and_granularity
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
    , toStartOfYear(ds) AS ds__year
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__year AS metric_time__year
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  SELECT
    COALESCE(subq_33.metric_time__year, subq_43.metric_time__year) AS metric_time__year
    , MAX(subq_33.month_start_bookings) AS month_start_bookings
    , MAX(subq_43.bookings_1_month_ago) AS bookings_1_month_ago
  FROM (
    SELECT
      metric_time__year
      , SUM(__bookings) AS month_start_bookings
    FROM (
      SELECT
        rss_28018_cte.ds__year AS metric_time__year
        , sma_28009_cte.__bookings AS __bookings
      FROM rss_28018_cte
      INNER JOIN
        sma_28009_cte
      ON
        toStartOfMonth(rss_28018_cte.ds__day) = sma_28009_cte.metric_time__day
      WHERE rss_28018_cte.ds__year = rss_28018_cte.ds__day
    ) subq_31
    GROUP BY
      metric_time__year
  ) subq_33
  FULL OUTER JOIN (
    SELECT
      rss_28018_cte.ds__year AS metric_time__year
      , SUM(sma_28009_cte.__bookings) AS bookings_1_month_ago
    FROM rss_28018_cte
    INNER JOIN
      sma_28009_cte
    ON
      addMonths(rss_28018_cte.ds__day, -1) = sma_28009_cte.metric_time__day
    GROUP BY
      rss_28018_cte.ds__year
  ) subq_43
  ON
    subq_33.metric_time__year = subq_43.metric_time__year
  GROUP BY
    COALESCE(subq_33.metric_time__year, subq_43.metric_time__year)
) subq_44
