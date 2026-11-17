test_name: test_join_to_base_on_multiple_shared_group_bys
test_filename: test_join_to_base_aggregated_outputs_to_sql.py
sql_engine: DuckDB
---
-- Join Aggregated Outputs to Base
WITH rss_28005_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  SELECT
    1 AS __listings
    , capacity AS __largest_listing
    , country AS listing__country_latest
    , is_lux AS listing__is_lux_latest
    , capacity AS listing__capacity_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

SELECT
  subq_8.listing__country_latest AS listing__country_latest
  , subq_8.listing__is_lux_latest AS listing__is_lux_latest
  , subq_8.listing__capacity_latest AS listing__capacity_latest
  , subq_8.__largest_listing AS __largest_listing
  , subq_11.__listings AS __listings
FROM (
  -- Read From CTE For node_id=rss_28005
  -- Select: ['__largest_listing', 'listing__country_latest', 'listing__is_lux_latest', 'listing__capacity_latest']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    listing__country_latest
    , listing__is_lux_latest
    , listing__capacity_latest
    , MAX(__largest_listing) AS __largest_listing
  FROM rss_28005_cte
  GROUP BY
    listing__country_latest
    , listing__is_lux_latest
    , listing__capacity_latest
) subq_8
LEFT OUTER JOIN (
  -- Read From CTE For node_id=rss_28005
  -- Select: ['__listings', 'listing__country_latest', 'listing__is_lux_latest']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    listing__country_latest
    , listing__is_lux_latest
    , SUM(__listings) AS __listings
  FROM rss_28005_cte
  GROUP BY
    listing__country_latest
    , listing__is_lux_latest
) subq_11
ON
  (
    (
      subq_8.listing__country_latest = subq_11.listing__country_latest
    ) OR (
      (
        subq_8.listing__country_latest IS NULL
      ) AND (
        subq_11.listing__country_latest IS NULL
      )
    )
  ) AND (
    (
      subq_8.listing__is_lux_latest = subq_11.listing__is_lux_latest
    ) OR (
      (
        subq_8.listing__is_lux_latest IS NULL
      ) AND (
        subq_11.listing__is_lux_latest IS NULL
      )
    )
  )
