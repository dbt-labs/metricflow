test_name: test_multi_hop_to_scd_dimension
test_filename: test_query_rendering.py
docstring:
  Tests conversion of a plan using an SCD dimension that is reached through another table.
sql_engine: DuckDB
---
-- Join Standard Outputs
-- Pass Only Elements: ['bookings', 'listing__lux_listing__is_confirmed_lux', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_14.metric_time__day AS metric_time__day
  , subq_19.lux_listing__is_confirmed_lux AS listing__lux_listing__is_confirmed_lux
  , SUM(subq_14.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_26000
) subq_14
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
) subq_19
ON
  (
    subq_14.listing = subq_19.listing
  ) AND (
    (
      subq_14.metric_time__day >= subq_19.lux_listing__window_start__day
    ) AND (
      (
        subq_14.metric_time__day < subq_19.lux_listing__window_end__day
      ) OR (
        subq_19.lux_listing__window_end__day IS NULL
      )
    )
  )
GROUP BY
  subq_14.metric_time__day
  , subq_19.lux_listing__is_confirmed_lux
