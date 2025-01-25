test_name: test_multi_hop_to_scd_dimension
test_filename: test_query_rendering.py
docstring:
  Tests conversion of a plan using an SCD dimension that is reached through another table.
sql_engine: Redshift
---
-- Join Standard Outputs
-- Pass Only Elements: ['bookings', 'listing__lux_listing__is_confirmed_lux', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  nr_subq_12.metric_time__day AS metric_time__day
  , nr_subq_14.lux_listing__is_confirmed_lux AS listing__lux_listing__is_confirmed_lux
  , SUM(nr_subq_12.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_26000
) nr_subq_12
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements: ['lux_listing__is_confirmed_lux', 'lux_listing__window_start__day', 'lux_listing__window_end__day', 'listing']
  SELECT
    lux_listings_src_26000.valid_from AS lux_listing__window_start__day
    , lux_listings_src_26000.valid_to AS lux_listing__window_end__day
    , lux_listing_mapping_src_26000.listing_id AS listing
    , lux_listings_src_26000.is_confirmed_lux AS lux_listing__is_confirmed_lux
  FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_26000
  LEFT OUTER JOIN
    ***************************.dim_lux_listings lux_listings_src_26000
  ON
    lux_listing_mapping_src_26000.lux_listing_id = lux_listings_src_26000.lux_listing_id
) nr_subq_14
ON
  (
    nr_subq_12.listing = nr_subq_14.listing
  ) AND (
    (
      nr_subq_12.metric_time__day >= nr_subq_14.lux_listing__window_start__day
    ) AND (
      (
        nr_subq_12.metric_time__day < nr_subq_14.lux_listing__window_end__day
      ) OR (
        nr_subq_14.lux_listing__window_end__day IS NULL
      )
    )
  )
GROUP BY
  nr_subq_12.metric_time__day
  , nr_subq_14.lux_listing__is_confirmed_lux
