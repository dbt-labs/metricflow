test_name: test_compute_metrics_node_ratio_from_multiple_semantic_models
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests the combine metrics node for ratio type metrics.
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  ds__day
  , listing__country_latest
  , CAST(bookings AS DOUBLE) / CAST(NULLIF(views, 0) AS DOUBLE) AS bookings_per_view
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_6.ds__day, nr_subq_13.ds__day) AS ds__day
    , COALESCE(nr_subq_6.listing__country_latest, nr_subq_13.listing__country_latest) AS listing__country_latest
    , MAX(nr_subq_6.bookings) AS bookings
    , MAX(nr_subq_13.views) AS views
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      nr_subq_0.ds__day AS ds__day
      , listings_latest_src_28000.country AS listing__country_latest
      , SUM(nr_subq_0.bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_0
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      nr_subq_0.listing = listings_latest_src_28000.listing_id
    GROUP BY
      nr_subq_0.ds__day
      , listings_latest_src_28000.country
  ) nr_subq_6
  FULL OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['views', 'listing__country_latest', 'ds__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      nr_subq_7.ds__day AS ds__day
      , listings_latest_src_28000.country AS listing__country_latest
      , SUM(nr_subq_7.views) AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , listing_id AS listing
        , 1 AS views
      FROM ***************************.fct_views views_source_src_28000
    ) nr_subq_7
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      nr_subq_7.listing = listings_latest_src_28000.listing_id
    GROUP BY
      nr_subq_7.ds__day
      , listings_latest_src_28000.country
  ) nr_subq_13
  ON
    (
      nr_subq_6.listing__country_latest = nr_subq_13.listing__country_latest
    ) AND (
      nr_subq_6.ds__day = nr_subq_13.ds__day
    )
  GROUP BY
    COALESCE(nr_subq_6.ds__day, nr_subq_13.ds__day)
    , COALESCE(nr_subq_6.listing__country_latest, nr_subq_13.listing__country_latest)
) nr_subq_14
