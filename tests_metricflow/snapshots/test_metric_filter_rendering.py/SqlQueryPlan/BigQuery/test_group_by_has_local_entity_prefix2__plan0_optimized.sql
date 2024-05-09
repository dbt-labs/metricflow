-- Constrain Output with WHERE
-- Pass Only Elements: ['listings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['listings', 'listing__view__listing__views']
  SELECT
    subq_26.view__listing__views AS listing__view__listing__views
    , subq_20.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'listing']
    SELECT
      listing_id AS listing
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_20
  LEFT OUTER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['view__listing', 'view__listing__views']
    SELECT
      view__listing
      , SUM(views) AS view__listing__views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['views', 'view__listing']
      SELECT
        listing_id AS view__listing
        , 1 AS views
      FROM ***************************.fct_views views_source_src_28000
    ) subq_23
    GROUP BY
      view__listing
  ) subq_26
  ON
    subq_20.listing = subq_26.view__listing
) subq_28
WHERE listing__view__listing__views > 2
