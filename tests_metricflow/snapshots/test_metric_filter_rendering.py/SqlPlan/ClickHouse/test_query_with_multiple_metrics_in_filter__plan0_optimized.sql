test_name: test_query_with_multiple_metrics_in_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with 2 simple metrics in the query-level where filter.
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    listing_id AS listing
    , 1 AS __bookings
    , guest_id AS __bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  SUM(__listings) AS listings
FROM (
  SELECT
    listings AS __listings
  FROM (
    SELECT
      subq_45.listing__bookings AS listing__bookings
      , subq_51.listing__bookers AS listing__bookers
      , subq_38.__listings AS listings
    FROM (
      SELECT
        listing_id AS listing
        , 1 AS __listings
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_38
    LEFT OUTER JOIN (
      SELECT
        listing
        , SUM(__bookings) AS listing__bookings
      FROM sma_28009_cte
      GROUP BY
        listing
    ) subq_45
    ON
      subq_38.listing = subq_45.listing
    LEFT OUTER JOIN (
      SELECT
        listing
        , COUNT(DISTINCT __bookers) AS listing__bookers
      FROM sma_28009_cte
      GROUP BY
        listing
    ) subq_51
    ON
      subq_38.listing = subq_51.listing
  ) subq_53
  WHERE listing__bookings > 2 AND listing__bookers > 1
) subq_55
