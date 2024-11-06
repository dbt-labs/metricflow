-- Join Standard Outputs
-- Pass Only Elements: ['bookings', 'listing__user__bio_added_ts__minute']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_20.user__bio_added_ts__minute AS listing__user__bio_added_ts__minute
  , SUM(subq_13.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds_partitioned, day) AS ds_partitioned__day
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_13
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements: ['user__ds_partitioned__day', 'user__bio_added_ts__minute', 'listing']
  SELECT
    DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, day) AS user__ds_partitioned__day
    , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, minute) AS user__bio_added_ts__minute
    , listings_latest_src_28000.listing_id AS listing
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  LEFT OUTER JOIN
    ***************************.dim_users users_ds_source_src_28000
  ON
    listings_latest_src_28000.user_id = users_ds_source_src_28000.user_id
) subq_20
ON
  (
    subq_13.listing = subq_20.listing
  ) AND (
    subq_13.ds_partitioned__day = subq_20.user__ds_partitioned__day
  )
GROUP BY
  listing__user__bio_added_ts__minute
