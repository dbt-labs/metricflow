-- Join Standard Outputs
-- Pass Only Elements:
--   ['bookings', 'listing__user__home_state_latest', 'metric_time']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_13.metric_time AS metric_time
  , subq_18.user__home_state_latest AS listing__user__home_state_latest
  , SUM(subq_13.bookings) AS bookings
FROM (
  -- Read Elements From Data Source 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['bookings', 'metric_time', 'listing']
  SELECT
    ds AS metric_time
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10018
) subq_13
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['user__home_state_latest', 'window_start', 'window_end', 'listing']
  SELECT
    listings_src_10020.active_from AS window_start
    , listings_src_10020.active_to AS window_end
    , listings_src_10020.listing_id AS listing
    , users_latest_src_10024.home_state_latest AS user__home_state_latest
  FROM ***************************.dim_listings listings_src_10020
  LEFT OUTER JOIN
    ***************************.dim_users_latest users_latest_src_10024
  ON
    listings_src_10020.user_id = users_latest_src_10024.user_id
) subq_18
ON
  (
    subq_13.listing = subq_18.listing
  ) AND (
    (
      subq_13.metric_time >= subq_18.window_start
    ) AND (
      (
        subq_13.metric_time < subq_18.window_end
      ) OR (
        subq_18.window_end IS NULL
      )
    )
  )
GROUP BY
  subq_13.metric_time
  , subq_18.user__home_state_latest
