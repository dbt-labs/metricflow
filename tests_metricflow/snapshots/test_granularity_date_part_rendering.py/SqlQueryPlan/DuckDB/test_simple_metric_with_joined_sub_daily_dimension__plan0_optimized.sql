-- Join Standard Outputs
-- Pass Only Elements: ['bookings', 'listing__user__bio_added_ts__minute']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_16.listing__user__bio_added_ts__minute AS listing__user__bio_added_ts__minute
  , SUM(subq_10.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings', 'ds_partitioned__day', 'listing']
  SELECT
    DATE_TRUNC('day', ds_partitioned) AS ds_partitioned__day
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_10
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements: ['user__ds_partitioned__day', 'user__bio_added_ts__minute', 'listing']
  SELECT
    listings_latest_src_28000.listing_id AS listing
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  LEFT OUTER JOIN
    ***************************.dim_users users_ds_source_src_28000
  ON
    listings_latest_src_28000.user_id = users_ds_source_src_28000.user_id
) subq_15
ON
  (
    subq_10.listing = subq_15.listing
  ) AND (
    subq_10.ds_partitioned__day = subq_15.ds_partitioned__day
  )
GROUP BY
  subq_16.listing__user__bio_added_ts__minute
