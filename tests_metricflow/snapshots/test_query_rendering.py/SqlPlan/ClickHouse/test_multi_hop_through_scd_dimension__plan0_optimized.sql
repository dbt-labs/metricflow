test_name: test_multi_hop_through_scd_dimension
test_filename: test_query_rendering.py
docstring:
  Tests conversion of a plan using a dimension that is reached through an SCD table.
sql_engine: ClickHouse
---
SELECT
  subq_25.metric_time__day AS metric_time__day
  , subq_30.user__home_state_latest AS listing__user__home_state_latest
  , SUM(subq_25.__bookings) AS bookings
FROM (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , listing_id AS listing
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_26000
) subq_25
LEFT OUTER JOIN (
  SELECT
    listings_src_26000.active_from AS window_start__day
    , listings_src_26000.active_to AS window_end__day
    , listings_src_26000.listing_id AS listing
    , users_latest_src_26000.home_state_latest AS user__home_state_latest
  FROM ***************************.dim_listings listings_src_26000
  LEFT OUTER JOIN
    ***************************.dim_users_latest users_latest_src_26000
  ON
    listings_src_26000.user_id = users_latest_src_26000.user_id
) subq_30
ON
  (
    subq_25.listing = subq_30.listing
  ) AND (
    (
      subq_25.metric_time__day >= subq_30.window_start__day
    ) AND (
      (
        subq_25.metric_time__day < subq_30.window_end__day
      ) OR (
        subq_30.window_end__day IS NULL
      )
    )
  )
GROUP BY
  subq_25.metric_time__day
  , subq_30.user__home_state_latest
