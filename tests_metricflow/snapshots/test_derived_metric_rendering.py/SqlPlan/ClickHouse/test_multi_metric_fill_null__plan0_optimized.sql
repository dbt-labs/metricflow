test_name: test_multi_metric_fill_null
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
SELECT
  COALESCE(subq_20.metric_time__day, subq_26.metric_time__day) AS metric_time__day
  , MAX(subq_20.twice_bookings_fill_nulls_with_0_without_time_spine) AS twice_bookings_fill_nulls_with_0_without_time_spine
  , MAX(subq_26.listings) AS listings
FROM (
  SELECT
    metric_time__day
    , 2 * bookings_fill_nulls_with_0_without_time_spine AS twice_bookings_fill_nulls_with_0_without_time_spine
  FROM (
    SELECT
      metric_time__day
      , COALESCE(__bookings_fill_nulls_with_0_without_time_spine, 0) AS bookings_fill_nulls_with_0_without_time_spine
    FROM (
      SELECT
        metric_time__day
        , SUM(__bookings_fill_nulls_with_0_without_time_spine) AS __bookings_fill_nulls_with_0_without_time_spine
      FROM (
        SELECT
          toStartOfDay(ds) AS metric_time__day
          , 1 AS __bookings_fill_nulls_with_0_without_time_spine
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_17
      GROUP BY
        metric_time__day
    ) subq_18
  ) subq_19
) subq_20
FULL OUTER JOIN (
  SELECT
    metric_time__day
    , SUM(__listings) AS listings
  FROM (
    SELECT
      toStartOfDay(created_at) AS metric_time__day
      , 1 AS __listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_24
  GROUP BY
    metric_time__day
) subq_26
ON
  subq_20.metric_time__day = subq_26.metric_time__day
GROUP BY
  COALESCE(subq_20.metric_time__day, subq_26.metric_time__day)
