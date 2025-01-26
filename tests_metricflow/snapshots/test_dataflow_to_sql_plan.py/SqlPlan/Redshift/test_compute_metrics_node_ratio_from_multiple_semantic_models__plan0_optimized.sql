test_name: test_compute_metrics_node_ratio_from_multiple_semantic_models
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests the combine metrics node for ratio type metrics.
sql_engine: Redshift
---
-- Compute Metrics via Expressions
WITH sma_28014_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , country AS country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

SELECT
  ds__day AS ds__day
  , listing__country_latest AS listing__country_latest
  , CAST(bookings AS DOUBLE PRECISION) / CAST(NULLIF(views, 0) AS DOUBLE PRECISION) AS bookings_per_view
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_26.ds__day, subq_34.ds__day) AS ds__day
    , COALESCE(subq_26.listing__country_latest, subq_34.listing__country_latest) AS listing__country_latest
    , MAX(subq_26.bookings) AS bookings
    , MAX(subq_34.views) AS views
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_19.ds__day AS ds__day
      , sma_28014_cte.country_latest AS listing__country_latest
      , SUM(subq_19.bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_19
    LEFT OUTER JOIN
      sma_28014_cte sma_28014_cte
    ON
      subq_19.listing = sma_28014_cte.listing
    GROUP BY
      subq_19.ds__day
      , sma_28014_cte.country_latest
  ) subq_26
  FULL OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['views', 'listing__country_latest', 'ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_28.ds__day AS ds__day
      , sma_28014_cte.country_latest AS listing__country_latest
      , SUM(subq_28.views) AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , listing_id AS listing
        , 1 AS views
      FROM ***************************.fct_views views_source_src_28000
    ) subq_28
    LEFT OUTER JOIN
      sma_28014_cte sma_28014_cte
    ON
      subq_28.listing = sma_28014_cte.listing
    GROUP BY
      subq_28.ds__day
      , sma_28014_cte.country_latest
  ) subq_34
  ON
    (
      subq_26.listing__country_latest = subq_34.listing__country_latest
    ) AND (
      subq_26.ds__day = subq_34.ds__day
    )
  GROUP BY
    COALESCE(subq_26.ds__day, subq_34.ds__day)
    , COALESCE(subq_26.listing__country_latest, subq_34.listing__country_latest)
) subq_35
