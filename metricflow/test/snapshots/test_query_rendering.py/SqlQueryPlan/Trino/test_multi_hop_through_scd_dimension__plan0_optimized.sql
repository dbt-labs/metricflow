-- Join Standard Outputs
-- Pass Only Elements:
--   ['bookings', 'listing__user__home_state_latest', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_13.metric_time__day AS metric_time__day
  , subq_18.user__home_state_latest AS listing__user__home_state_latest
  , SUM(subq_13.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['bookings', 'metric_time__day', 'listing']
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10015
) subq_13
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['user__home_state_latest', 'window_start__day', 'window_end__day', 'listing']
  SELECT
    listings_src_10017.active_from AS window_start__day
    , listings_src_10017.active_to AS window_end__day
    , listings_src_10017.listing_id AS listing
    , users_latest_src_10021.home_state_latest AS user__home_state_latest
  FROM ***************************.dim_listings listings_src_10017
  LEFT OUTER JOIN
    ***************************.dim_users_latest users_latest_src_10021
  ON
    listings_src_10017.user_id = users_latest_src_10021.user_id
) subq_18
ON
  (
    subq_13.listing = subq_18.listing
  ) AND (
    (
      subq_13.metric_time__day >= subq_18.window_start__day
    ) AND (
      (
        subq_13.metric_time__day < subq_18.window_end__day
      ) OR (
        subq_18.window_end__day IS NULL
      )
    )
  )
GROUP BY
  subq_13.metric_time__day
  , subq_18.user__home_state_latest
