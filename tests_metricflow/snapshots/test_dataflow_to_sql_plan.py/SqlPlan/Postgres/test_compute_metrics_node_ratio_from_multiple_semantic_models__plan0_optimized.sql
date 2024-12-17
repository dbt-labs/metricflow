test_name: test_compute_metrics_node_ratio_from_multiple_semantic_models
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests the combine metrics node for ratio type metrics.
sql_engine: Postgres
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
    COALESCE(subq_27.ds__day, subq_35.ds__day) AS ds__day
    , COALESCE(subq_27.listing__country_latest, subq_35.listing__country_latest) AS listing__country_latest
    , MAX(subq_27.bookings) AS bookings
    , MAX(subq_35.views) AS views
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_20.ds__day AS ds__day
      , sma_28014_cte.country_latest AS listing__country_latest
      , SUM(subq_20.bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_20
    LEFT OUTER JOIN
      sma_28014_cte sma_28014_cte
    ON
      subq_20.listing = sma_28014_cte.listing
    GROUP BY
      subq_20.ds__day
      , sma_28014_cte.country_latest
  ) subq_27
  FULL OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['views', 'listing__country_latest', 'ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_29.ds__day AS ds__day
      , sma_28014_cte.country_latest AS listing__country_latest
      , SUM(subq_29.views) AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , listing_id AS listing
        , 1 AS views
      FROM ***************************.fct_views views_source_src_28000
    ) subq_29
    LEFT OUTER JOIN
      sma_28014_cte sma_28014_cte
    ON
      subq_29.listing = sma_28014_cte.listing
    GROUP BY
      subq_29.ds__day
      , sma_28014_cte.country_latest
  ) subq_35
  ON
    (
      subq_27.listing__country_latest = subq_35.listing__country_latest
    ) AND (
      subq_27.ds__day = subq_35.ds__day
    )
  GROUP BY
    COALESCE(subq_27.ds__day, subq_35.ds__day)
    , COALESCE(subq_27.listing__country_latest, subq_35.listing__country_latest)
) subq_36
