test_name: test_nested_offsets_with_where_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
WITH rss_28018_cte AS (
  SELECT
    ds AS ds__day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  SELECT
    metric_time__day
    , bookings_offset_once
  FROM (
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_30.bookings_offset_once AS bookings_offset_once
    FROM rss_28018_cte
    INNER JOIN (
      SELECT
        metric_time__day
        , 2 * bookings AS bookings_offset_once
      FROM (
        SELECT
          rss_28018_cte.ds__day AS metric_time__day
          , subq_23.__bookings AS bookings
        FROM rss_28018_cte
        INNER JOIN (
          SELECT
            metric_time__day
            , SUM(__bookings) AS __bookings
          FROM (
            SELECT
              toStartOfDay(ds) AS metric_time__day
              , 1 AS __bookings
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_22
          GROUP BY
            metric_time__day
        ) subq_23
        ON
          addDays(rss_28018_cte.ds__day, -5) = subq_23.metric_time__day
      ) subq_29
    ) subq_30
    ON
      addDays(rss_28018_cte.ds__day, -2) = subq_30.metric_time__day
  ) subq_35
  WHERE metric_time__day = '2020-01-12' or metric_time__day = '2020-01-13'
) subq_36
