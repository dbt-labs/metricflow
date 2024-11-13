test_name: test_compute_metrics_node_ratio_from_multiple_semantic_models
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests the combine metrics node for ratio type metrics.
sql_engine: BigQuery
---
-- Read From CTE For node_id=cm_2
WITH sma_28014_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , country AS country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

, cm_0_cte AS (
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
      DATETIME_TRUNC(ds, day) AS ds__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_20
  LEFT OUTER JOIN
    sma_28014_cte sma_28014_cte
  ON
    subq_20.listing = sma_28014_cte.listing
  GROUP BY
    ds__day
    , listing__country_latest
)

, cm_1_cte AS (
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
      DATETIME_TRUNC(ds, day) AS ds__day
      , listing_id AS listing
      , 1 AS views
    FROM ***************************.fct_views views_source_src_28000
  ) subq_29
  LEFT OUTER JOIN
    sma_28014_cte sma_28014_cte
  ON
    subq_29.listing = sma_28014_cte.listing
  GROUP BY
    ds__day
    , listing__country_latest
)

, cm_2_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    ds__day
    , listing__country_latest
    , CAST(bookings AS FLOAT64) / CAST(NULLIF(views, 0) AS FLOAT64) AS bookings_per_view
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_0_cte.ds__day, cm_1_cte.ds__day) AS ds__day
      , COALESCE(cm_0_cte.listing__country_latest, cm_1_cte.listing__country_latest) AS listing__country_latest
      , MAX(cm_0_cte.bookings) AS bookings
      , MAX(cm_1_cte.views) AS views
    FROM cm_0_cte cm_0_cte
    FULL OUTER JOIN
      cm_1_cte cm_1_cte
    ON
      (
        cm_0_cte.listing__country_latest = cm_1_cte.listing__country_latest
      ) AND (
        cm_0_cte.ds__day = cm_1_cte.ds__day
      )
    GROUP BY
      ds__day
      , listing__country_latest
  ) subq_36
)

SELECT
  ds__day AS ds__day
  , listing__country_latest AS listing__country_latest
  , bookings_per_view AS bookings_per_view
FROM cm_2_cte cm_2_cte
