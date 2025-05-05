test_name: test_multi_hop_through_scd_dimension
test_filename: test_query_rendering.py
docstring:
  Tests conversion of a plan using a dimension that is reached through an SCD table.
sql_engine: BigQuery
---
-- Join Standard Outputs
-- Pass Only Elements: ['bookings', 'listing__user__home_state_latest', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_21.metric_time__day AS metric_time__day
  , subq_26.user__home_state_latest AS listing__user__home_state_latest
  , SUM(subq_21.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_26000
) subq_21
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements: ['user__home_state_latest', 'window_start__day', 'window_end__day', 'listing']
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
) subq_26
ON
  (
    subq_21.listing = subq_26.listing
  ) AND (
    (
      subq_21.metric_time__day >= subq_26.window_start__day
    ) AND (
      (
        subq_21.metric_time__day < subq_26.window_end__day
      ) OR (
        subq_26.window_end__day IS NULL
      )
    )
  )
GROUP BY
  metric_time__day
  , listing__user__home_state_latest
