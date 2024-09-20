-- Pass Only Elements: ['bookings', 'listing__ds__day']
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['bookings', 'listing__ds__martian_day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_18.martian_day AS listing__ds__martian_day
  , SUM(subq_17.bookings) AS bookings
FROM (
  -- Join Standard Outputs
  SELECT
    DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__ds__day
    , subq_13.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'listing']
    SELECT
      listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_13
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_13.listing = listings_latest_src_28000.listing_id
) subq_17
LEFT OUTER JOIN
  ***************************.mf_time_spine subq_18
ON
  subq_17.listing__ds__day = subq_18.ds
GROUP BY
  subq_18.martian_day
