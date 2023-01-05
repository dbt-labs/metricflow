-- Join Standard Outputs
-- Pass Only Elements:
--   ['bookings', 'listing__lux_listing__is_confirmed_lux', 'metric_time']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_13.metric_time AS metric_time
  , subq_18.lux_listing__is_confirmed_lux AS listing__lux_listing__is_confirmed_lux
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
  --   ['lux_listing__is_confirmed_lux',
  --    'lux_listing__window_start',
  --    'lux_listing__window_end',
  --    'listing']
  SELECT
    lux_listings_src_10022.valid_from AS lux_listing__window_start
    , lux_listings_src_10022.valid_to AS lux_listing__window_end
    , lux_listing_mapping_src_10021.listing_id AS listing
    , lux_listings_src_10022.is_confirmed_lux AS lux_listing__is_confirmed_lux
  FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_10021
  LEFT OUTER JOIN
    ***************************.dim_lux_listings lux_listings_src_10022
  ON
    lux_listing_mapping_src_10021.lux_listing_id = lux_listings_src_10022.lux_listing_id
) subq_18
ON
  (
    subq_13.listing = subq_18.listing
  ) AND (
    (
      subq_13.metric_time >= subq_18.lux_listing__window_start
    ) AND (
      (
        subq_13.metric_time < subq_18.lux_listing__window_end
      ) OR (
        subq_18.lux_listing__window_end IS NULL
      )
    )
  )
GROUP BY
  subq_13.metric_time
  , subq_18.lux_listing__is_confirmed_lux
