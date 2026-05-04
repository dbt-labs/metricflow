test_name: test_join_to_time_spine_with_filter_not_in_group_by_using_agg_time
test_filename: test_fill_nulls_with_rendering.py
sql_engine: ClickHouse
---
SELECT
  subq_23.booking__ds__day AS booking__ds__day
  , subq_18.__bookings_join_to_time_spine_with_tiered_filters AS bookings_join_to_time_spine_with_tiered_filters
FROM (
  SELECT
    booking__ds__day
  FROM (
    SELECT
      ds AS booking__ds__day
      , ds AS metric_time__day
      , toStartOfMonth(ds) AS booking__ds__month
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) subq_21
  WHERE (
    metric_time__day >= '2020-01-02'
  ) AND (
    metric_time__day <= '2020-01-02'
  ) AND (
    booking__ds__month > '2020-01-01'
  )
) subq_23
LEFT OUTER JOIN (
  SELECT
    booking__ds__day
    , SUM(__bookings_join_to_time_spine_with_tiered_filters) AS __bookings_join_to_time_spine_with_tiered_filters
  FROM (
    SELECT
      booking__ds__day
      , bookings_join_to_time_spine_with_tiered_filters AS __bookings_join_to_time_spine_with_tiered_filters
    FROM (
      SELECT
        toStartOfDay(ds) AS booking__ds__day
        , toStartOfMonth(ds) AS booking__ds__month
        , toStartOfDay(ds) AS metric_time__day
        , 1 AS bookings_join_to_time_spine_with_tiered_filters
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_15
    WHERE (
      metric_time__day >= '2020-01-02'
    ) AND (
      metric_time__day <= '2020-01-02'
    ) AND (
      booking__ds__month > '2020-01-01'
    )
  ) subq_17
  GROUP BY
    booking__ds__day
) subq_18
ON
  subq_23.booking__ds__day = subq_18.booking__ds__day
