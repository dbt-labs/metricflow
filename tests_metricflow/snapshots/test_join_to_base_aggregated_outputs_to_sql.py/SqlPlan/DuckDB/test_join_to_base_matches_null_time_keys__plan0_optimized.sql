test_name: test_join_to_base_matches_null_time_keys
test_filename: test_join_to_base_aggregated_outputs_to_sql.py
sql_engine: DuckDB
---
-- Join Aggregated Outputs to Base
WITH rss_28005_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  SELECT
    1 AS __listings
    , capacity AS __largest_listing
    , DATE_TRUNC('day', created_at) AS listing__ds__day
    , country AS listing__country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

SELECT
  subq_8.listing__ds__day AS listing__ds__day
  , subq_8.listing__country_latest AS listing__country_latest
  , subq_8.__largest_listing AS __largest_listing
  , subq_11.__listings AS __listings
FROM (
  -- Read From CTE For node_id=rss_28005
  -- Select: ['__largest_listing', 'listing__country_latest', 'listing__ds__day']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    listing__ds__day
    , listing__country_latest
    , MAX(__largest_listing) AS __largest_listing
  FROM rss_28005_cte
  GROUP BY
    listing__ds__day
    , listing__country_latest
) subq_8
LEFT OUTER JOIN (
  -- Read From CTE For node_id=rss_28005
  -- Select: ['__listings', 'listing__country_latest', 'listing__ds__day']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    listing__ds__day
    , listing__country_latest
    , SUM(__listings) AS __listings
  FROM rss_28005_cte
  GROUP BY
    listing__ds__day
    , listing__country_latest
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
      subq_8.listing__ds__day = subq_11.listing__ds__day
    ) OR (
      (
        subq_8.listing__ds__day IS NULL
      ) AND (
        subq_11.listing__ds__day IS NULL
      )
    )
  )
