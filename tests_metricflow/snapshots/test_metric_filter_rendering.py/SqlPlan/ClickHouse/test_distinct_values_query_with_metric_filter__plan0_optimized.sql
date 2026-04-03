test_name: test_distinct_values_query_with_metric_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a distinct values query with a metric in the query-level where filter.
sql_engine: ClickHouse
---
SELECT
  listing
FROM (
  SELECT
    lux_listing_mapping_src_28000.listing_id AS listing
    , subq_27.listing__bookings AS listing__bookings
  FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_28000
  FULL OUTER JOIN (
    SELECT
      listing
      , SUM(__bookings) AS listing__bookings
    FROM (
      SELECT
        listing_id AS listing
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_24
    GROUP BY
      listing
  ) subq_27
  ON
    lux_listing_mapping_src_28000.listing_id = subq_27.listing
) subq_29
WHERE listing__bookings > 2
GROUP BY
  listing
