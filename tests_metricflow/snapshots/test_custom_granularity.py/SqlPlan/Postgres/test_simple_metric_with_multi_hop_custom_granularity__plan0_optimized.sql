test_name: test_simple_metric_with_multi_hop_custom_granularity
test_filename: test_custom_granularity.py
docstring:
  Test simple metric with a multi hop dimension and custom grain.
sql_engine: Postgres
---
-- Join Standard Outputs
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['bookings', 'listing__user__ds__martian_day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_22.martian_day AS listing__user__ds__martian_day
  , SUM(subq_14.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds_partitioned) AS ds_partitioned__day
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_14
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements: ['user__ds_partitioned__day', 'user__ds__day', 'listing']
  SELECT
    DATE_TRUNC('day', users_ds_source_src_28000.ds) AS user__ds__day
    , DATE_TRUNC('day', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__day
    , listings_latest_src_28000.listing_id AS listing
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  LEFT OUTER JOIN
    ***************************.dim_users users_ds_source_src_28000
  ON
    listings_latest_src_28000.user_id = users_ds_source_src_28000.user_id
) subq_21
ON
  (
    subq_14.listing = subq_21.listing
  ) AND (
    subq_14.ds_partitioned__day = subq_21.user__ds_partitioned__day
  )
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_22
ON
  subq_21.user__ds__day = subq_22.ds
GROUP BY
  subq_22.martian_day
