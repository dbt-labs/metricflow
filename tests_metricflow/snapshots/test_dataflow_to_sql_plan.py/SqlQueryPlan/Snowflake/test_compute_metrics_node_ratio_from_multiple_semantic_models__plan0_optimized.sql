test_name: test_compute_metrics_node_ratio_from_multiple_semantic_models
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests the combine metrics node for ratio type metrics.
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
SELECT
  ds__day
  , listing__country_latest
  , CAST(bookings AS DOUBLE) / CAST(NULLIF(views, 0) AS DOUBLE) AS bookings_per_view
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_27.ds__day, subq_36.ds__day) AS ds__day
    , COALESCE(subq_27.listing__country_latest, subq_36.listing__country_latest) AS listing__country_latest
    , MAX(subq_27.bookings) AS bookings
    , MAX(subq_36.views) AS views
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_20.ds__day AS ds__day
      , listings_latest_src_28000.country AS listing__country_latest
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
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_20.listing = listings_latest_src_28000.listing_id
    GROUP BY
      subq_20.ds__day
      , listings_latest_src_28000.country
  ) subq_27
  FULL OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['views', 'listing__country_latest', 'ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_29.ds__day AS ds__day
      , listings_latest_src_28000.country AS listing__country_latest
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
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_29.listing = listings_latest_src_28000.listing_id
    GROUP BY
      subq_29.ds__day
      , listings_latest_src_28000.country
  ) subq_36
  ON
    (
      subq_27.listing__country_latest = subq_36.listing__country_latest
    ) AND (
      subq_27.ds__day = subq_36.ds__day
    )
  GROUP BY
    COALESCE(subq_27.ds__day, subq_36.ds__day)
    , COALESCE(subq_27.listing__country_latest, subq_36.listing__country_latest)
) subq_37
