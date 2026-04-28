test_name: test_query_with_ratio_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a ratio metric in the query-level where filter.
sql_engine: ClickHouse
---
SELECT
  SUM(__listings) AS listings
FROM (
  SELECT
    listings AS __listings
  FROM (
    SELECT
      CAST(subq_51.bookings AS Nullable(Float64)) / CAST(NULLIF(subq_51.bookers, 0) AS Nullable(Float64)) AS listing__bookings_per_booker
      , subq_45.__listings AS listings
    FROM (
      SELECT
        listing_id AS listing
        , 1 AS __listings
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_45
    LEFT OUTER JOIN (
      SELECT
        listing
        , SUM(__bookings) AS bookings
        , COUNT(DISTINCT __bookers) AS bookers
      FROM (
        SELECT
          listing_id AS listing
          , 1 AS __bookings
          , guest_id AS __bookers
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_49
      GROUP BY
        listing
    ) subq_51
    ON
      subq_45.listing = subq_51.listing
  ) subq_55
  WHERE listing__bookings_per_booker > 1
) subq_57
