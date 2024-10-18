-- Pass Only Elements: ['bookings', 'listing__user__ds__day']
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['bookings', 'listing__user__ds__martian_day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_26.martian_day AS listing__user__ds__martian_day
  , SUM(subq_25.bookings) AS bookings
FROM (
  -- Join Standard Outputs
  SELECT
    subq_24.user__ds__day AS listing__user__ds__day
    , subq_17.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'ds_partitioned__day', 'listing']
    SELECT
      DATETIME_TRUNC(ds_partitioned, day) AS ds_partitioned__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_17
  LEFT OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['user__ds_partitioned__day', 'user__ds__day', 'listing']
    SELECT
      DATETIME_TRUNC(users_ds_source_src_28000.ds, day) AS user__ds__day
      , DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, day) AS user__ds_partitioned__day
      , listings_latest_src_28000.listing_id AS listing
    FROM ***************************.dim_listings_latest listings_latest_src_28000
    LEFT OUTER JOIN
      ***************************.dim_users users_ds_source_src_28000
    ON
      listings_latest_src_28000.user_id = users_ds_source_src_28000.user_id
  ) subq_24
  ON
    (
      subq_17.listing = subq_24.listing
    ) AND (
      subq_17.ds_partitioned__day = subq_24.user__ds_partitioned__day
    )
) subq_25
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_26
ON
  subq_25.listing__user__ds__day = subq_26.ds
GROUP BY
  listing__user__ds__martian_day
