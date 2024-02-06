-- Join Standard Outputs
-- Pass Only Elements: ['bookings', 'listing__user__home_state_latest', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_24.metric_time__day AS metric_time__day
  , subq_29.user__home_state_latest AS listing__user__home_state_latest
  , SUM(subq_24.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
  SELECT
    DATE_TRUNC(ds, day) AS metric_time__day
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_26000
) subq_24
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
) subq_29
ON
  (
    subq_24.listing = subq_29.listing
  ) AND (
    (
      subq_24.metric_time__day >= subq_29.window_start__day
    ) AND (
      (
        subq_24.metric_time__day < subq_29.window_end__day
      ) OR (
        subq_29.window_end__day IS NULL
      )
    )
  )
GROUP BY
  metric_time__day
  , listing__user__home_state_latest
