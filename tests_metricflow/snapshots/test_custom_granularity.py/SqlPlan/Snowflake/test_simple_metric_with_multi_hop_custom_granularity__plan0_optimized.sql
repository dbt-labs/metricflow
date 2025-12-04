test_name: test_simple_metric_with_multi_hop_custom_granularity
test_filename: test_custom_granularity.py
docstring:
  Test simple metric with a multi hop dimension and custom grain.
sql_engine: Snowflake
---
-- Join Standard Outputs
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['__bookings', 'listing__user__ds__alien_day']
-- Pass Only Elements: ['__bookings', 'listing__user__ds__alien_day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_48.alien_day AS listing__user__ds__alien_day
  , SUM(subq_40.__bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds_partitioned) AS ds_partitioned__day
    , listing_id AS listing
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_40
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
) subq_47
ON
  (
    subq_40.listing = subq_47.listing
  ) AND (
    subq_40.ds_partitioned__day = subq_47.user__ds_partitioned__day
  )
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_48
ON
  subq_47.user__ds__day = subq_48.ds
GROUP BY
  subq_48.alien_day
