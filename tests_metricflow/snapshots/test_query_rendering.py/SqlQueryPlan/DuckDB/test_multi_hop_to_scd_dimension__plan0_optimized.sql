-- Join Standard Outputs
-- Pass Only Elements: ['bookings', 'listing__lux_listing__is_confirmed_lux', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_8.metric_time__day AS metric_time__day
  , subq_12.listing__lux_listing__is_confirmed_lux AS listing__lux_listing__is_confirmed_lux
  , SUM(subq_8.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_26000
) subq_8
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements: ['lux_listing__is_confirmed_lux', 'lux_listing__window_start__day', 'lux_listing__window_end__day', 'listing']
  SELECT
    lux_listing_mapping_src_26000.listing_id AS listing
  FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_26000
  LEFT OUTER JOIN
    ***************************.dim_lux_listings lux_listings_src_26000
  ON
    lux_listing_mapping_src_26000.lux_listing_id = lux_listings_src_26000.lux_listing_id
) subq_11
ON
  (
    subq_8.listing = subq_11.listing
  ) AND (
    (
      subq_8.metric_time__day >= subq_11.window_start__day
    ) AND (
      (
        subq_8.metric_time__day < subq_11.window_end__day
      ) OR (
        subq_11.window_end__day IS NULL
      )
    )
  )
GROUP BY
  subq_8.metric_time__day
  , subq_12.listing__lux_listing__is_confirmed_lux
