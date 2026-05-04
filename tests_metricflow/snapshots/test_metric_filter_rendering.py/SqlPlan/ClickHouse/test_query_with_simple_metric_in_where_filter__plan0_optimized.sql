test_name: test_query_with_simple_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a simple metric in the query-level where filter.
sql_engine: ClickHouse
---
SELECT
  SUM(__listings) AS listings
FROM (
  SELECT
    listings AS __listings
  FROM (
    SELECT
      subq_31.listing__bookings AS listing__bookings
      , subq_24.__listings AS listings
    FROM (
      SELECT
        listing_id AS listing
        , 1 AS __listings
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_24
    LEFT OUTER JOIN (
      SELECT
        listing
        , SUM(__bookings) AS listing__bookings
      FROM (
        SELECT
          listing_id AS listing
          , 1 AS __bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_28
      GROUP BY
        listing
    ) subq_31
    ON
      subq_24.listing = subq_31.listing
  ) subq_33
  WHERE listing__bookings > 2
) subq_35
