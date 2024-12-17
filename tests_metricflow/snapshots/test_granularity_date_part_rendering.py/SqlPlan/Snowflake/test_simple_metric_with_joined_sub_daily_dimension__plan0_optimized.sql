test_name: test_simple_metric_with_joined_sub_daily_dimension
test_filename: test_granularity_date_part_rendering.py
sql_engine: Snowflake
---
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
    DATE_TRUNC('day', ds_partitioned) AS ds_partitioned__day
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_13
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements: ['user__ds_partitioned__day', 'user__bio_added_ts__minute', 'listing']
  SELECT
    DATE_TRUNC('day', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__day
    , DATE_TRUNC('minute', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__minute
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
  subq_20.user__bio_added_ts__minute
