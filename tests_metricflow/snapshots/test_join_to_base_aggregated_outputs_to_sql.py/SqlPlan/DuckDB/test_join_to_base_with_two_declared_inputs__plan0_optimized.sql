test_name: test_join_to_base_with_two_declared_inputs
test_filename: test_join_to_base_aggregated_outputs_to_sql.py
sql_engine: DuckDB
---
-- Join Aggregated Outputs to Base
WITH rss_28005_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  SELECT
    1 AS __listings
    , capacity AS __smallest_listing
    , capacity AS __largest_listing
    , country AS listing__country_latest
    , is_lux AS listing__is_lux_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

SELECT
  subq_11.listing__country_latest AS listing__country_latest
  , subq_11.listing__is_lux_latest AS listing__is_lux_latest
  , subq_11.__largest_listing AS __largest_listing
  , subq_14.__listings AS __listings
  , subq_17.__smallest_listing AS __smallest_listing
FROM (
  -- Read From CTE For node_id=rss_28005
  -- Select: ['__largest_listing', 'listing__country_latest', 'listing__is_lux_latest']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    listing__country_latest
    , listing__is_lux_latest
    , MAX(__largest_listing) AS __largest_listing
  FROM rss_28005_cte
  GROUP BY
    listing__country_latest
    , listing__is_lux_latest
) subq_11
LEFT OUTER JOIN (
  -- Read From CTE For node_id=rss_28005
  -- Select: ['__listings', 'listing__country_latest']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    listing__country_latest
    , SUM(__listings) AS __listings
  FROM rss_28005_cte
  GROUP BY
    listing__country_latest
) subq_14
ON
  (
    subq_11.listing__country_latest = subq_14.listing__country_latest
  ) OR (
    (
      subq_11.listing__country_latest IS NULL
    ) AND (
      subq_14.listing__country_latest IS NULL
    )
  )
LEFT OUTER JOIN (
  -- Read From CTE For node_id=rss_28005
  -- Select: ['__smallest_listing', 'listing__is_lux_latest']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    listing__is_lux_latest
    , MIN(__smallest_listing) AS __smallest_listing
  FROM rss_28005_cte
  GROUP BY
    listing__is_lux_latest
) subq_17
ON
  (
    subq_11.listing__is_lux_latest = subq_17.listing__is_lux_latest
  ) OR (
    (
      subq_11.listing__is_lux_latest IS NULL
    ) AND (
      subq_17.listing__is_lux_latest IS NULL
    )
  )
