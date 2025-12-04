test_name: test_compute_metrics_node_ratio_from_multiple_semantic_models
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests the combine metrics node for ratio type metrics.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
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
  , CAST(bookings AS DOUBLE) / CAST(NULLIF(views, 0) AS DOUBLE) AS bookings_per_view
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_30.ds__day, subq_39.ds__day) AS ds__day
    , COALESCE(subq_30.listing__country_latest, subq_39.listing__country_latest) AS listing__country_latest
    , MAX(subq_30.bookings) AS bookings
    , MAX(subq_39.views) AS views
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['__bookings', 'listing__country_latest', 'ds__day']
    -- Pass Only Elements: ['__bookings', 'listing__country_latest', 'ds__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      subq_22.ds__day AS ds__day
      , sma_28014_cte.country_latest AS listing__country_latest
      , SUM(subq_22.__bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , listing_id AS listing
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_22
    LEFT OUTER JOIN
      sma_28014_cte
    ON
      subq_22.listing = sma_28014_cte.listing
    GROUP BY
      subq_22.ds__day
      , sma_28014_cte.country_latest
  ) subq_30
  FULL OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['__views', 'listing__country_latest', 'ds__day']
    -- Pass Only Elements: ['__views', 'listing__country_latest', 'ds__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      subq_32.ds__day AS ds__day
      , sma_28014_cte.country_latest AS listing__country_latest
      , SUM(subq_32.__views) AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , listing_id AS listing
        , 1 AS __views
      FROM ***************************.fct_views views_source_src_28000
    ) subq_32
    LEFT OUTER JOIN
      sma_28014_cte
    ON
      subq_32.listing = sma_28014_cte.listing
    GROUP BY
      subq_32.ds__day
      , sma_28014_cte.country_latest
  ) subq_39
  ON
    (
      subq_30.listing__country_latest = subq_39.listing__country_latest
    ) AND (
      subq_30.ds__day = subq_39.ds__day
    )
  GROUP BY
    COALESCE(subq_30.ds__day, subq_39.ds__day)
    , COALESCE(subq_30.listing__country_latest, subq_39.listing__country_latest)
) subq_40
