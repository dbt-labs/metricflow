test_name: test_simple_metric_with_joined_sub_daily_dimension
test_filename: test_granularity_date_part_rendering.py
sql_engine: ClickHouse
---
SELECT
  subq_46.user__bio_added_ts__minute AS listing__user__bio_added_ts__minute
  , SUM(subq_39.__bookings) AS bookings
FROM (
  SELECT
    toStartOfDay(ds_partitioned) AS ds_partitioned__day
    , listing_id AS listing
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_39
LEFT OUTER JOIN (
  SELECT
    toStartOfDay(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__day
    , toStartOfMinute(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__minute
    , listings_latest_src_28000.listing_id AS listing
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  LEFT OUTER JOIN
    ***************************.dim_users users_ds_source_src_28000
  ON
    listings_latest_src_28000.user_id = users_ds_source_src_28000.user_id
) subq_46
ON
  (
    subq_39.listing = subq_46.listing
  ) AND (
    subq_39.ds_partitioned__day = subq_46.user__ds_partitioned__day
  )
GROUP BY
  subq_46.user__bio_added_ts__minute
