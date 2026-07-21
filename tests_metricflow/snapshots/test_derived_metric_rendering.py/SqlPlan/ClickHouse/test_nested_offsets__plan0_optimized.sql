test_name: test_nested_offsets
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
    rss_28018_cte.ds__day AS metric_time__day
    , subq_29.bookings_offset_once AS bookings_offset_once
  FROM rss_28018_cte
  INNER JOIN (
    SELECT
      metric_time__day
      , 2 * bookings AS bookings_offset_once
    FROM (
      SELECT
        rss_28018_cte.ds__day AS metric_time__day
        , subq_22.__bookings AS bookings
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
        ) subq_21
        GROUP BY
          metric_time__day
      ) subq_22
      ON
        addDays(rss_28018_cte.ds__day, -5) = subq_22.metric_time__day
    ) subq_28
  ) subq_29
  ON
    addDays(rss_28018_cte.ds__day, -2) = subq_29.metric_time__day
) subq_34
