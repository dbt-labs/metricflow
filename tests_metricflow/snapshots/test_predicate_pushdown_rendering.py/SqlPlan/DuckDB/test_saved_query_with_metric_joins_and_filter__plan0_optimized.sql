test_name: test_saved_query_with_metric_joins_and_filter
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we join to a time spine and query the filter input.

      This should produce a SQL query that applies the filter outside of the time spine join.
sql_engine: DuckDB
---
-- Combine Aggregated Outputs
WITH sma_28014_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , is_lux AS is_lux_latest
    , capacity AS capacity_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

, cm_8_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'listing__capacity_latest']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    listing__capacity_latest
    , SUM(bookings) AS bookings
  FROM (
    -- Join Standard Outputs
    SELECT
      sma_28014_cte.is_lux_latest AS listing__is_lux_latest
      , sma_28014_cte.capacity_latest AS listing__capacity_latest
      , subq_31.metric_time__day AS metric_time__day
      , subq_31.bookings AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_31
    LEFT OUTER JOIN
      sma_28014_cte sma_28014_cte
    ON
      subq_31.listing = sma_28014_cte.listing
  ) subq_35
  WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
  GROUP BY
    listing__capacity_latest
)

, sma_28018_cte AS (
  -- Read Elements From Semantic Model 'views_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , 1 AS views
  FROM ***************************.fct_views views_source_src_28000
)

SELECT
  COALESCE(cm_8_cte.listing__capacity_latest, subq_48.listing__capacity_latest, subq_58.listing__capacity_latest) AS listing__capacity_latest
  , MAX(cm_8_cte.bookings) AS bookings
  , MAX(subq_48.views) AS views
  , MAX(CAST(subq_58.bookings AS DOUBLE) / CAST(NULLIF(subq_58.views, 0) AS DOUBLE)) AS bookings_per_view
FROM cm_8_cte cm_8_cte
FULL OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['views', 'listing__capacity_latest']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    listing__capacity_latest
    , SUM(views) AS views
  FROM (
    -- Join Standard Outputs
    SELECT
      sma_28014_cte.is_lux_latest AS listing__is_lux_latest
      , sma_28014_cte.capacity_latest AS listing__capacity_latest
      , sma_28018_cte.metric_time__day AS metric_time__day
      , sma_28018_cte.views AS views
    FROM sma_28018_cte sma_28018_cte
    LEFT OUTER JOIN
      sma_28014_cte sma_28014_cte
    ON
      sma_28018_cte.listing = sma_28014_cte.listing
  ) subq_44
  WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
  GROUP BY
    listing__capacity_latest
) subq_48
ON
  cm_8_cte.listing__capacity_latest = subq_48.listing__capacity_latest
FULL OUTER JOIN (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(cm_8_cte.listing__capacity_latest, subq_57.listing__capacity_latest) AS listing__capacity_latest
    , MAX(cm_8_cte.bookings) AS bookings
    , MAX(subq_57.views) AS views
  FROM cm_8_cte cm_8_cte
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['views', 'listing__capacity_latest']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      listing__capacity_latest
      , SUM(views) AS views
    FROM (
      -- Join Standard Outputs
      SELECT
        sma_28014_cte.is_lux_latest AS listing__is_lux_latest
        , sma_28014_cte.capacity_latest AS listing__capacity_latest
        , sma_28018_cte.metric_time__day AS metric_time__day
        , sma_28018_cte.views AS views
      FROM sma_28018_cte sma_28018_cte
      LEFT OUTER JOIN
        sma_28014_cte sma_28014_cte
      ON
        sma_28018_cte.listing = sma_28014_cte.listing
    ) subq_53
    WHERE (((listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')) AND (listing__is_lux_latest)) AND (metric_time__day >= '2020-01-02')
    GROUP BY
      listing__capacity_latest
  ) subq_57
  ON
    cm_8_cte.listing__capacity_latest = subq_57.listing__capacity_latest
  GROUP BY
    COALESCE(cm_8_cte.listing__capacity_latest, subq_57.listing__capacity_latest)
) subq_58
ON
  COALESCE(cm_8_cte.listing__capacity_latest, subq_48.listing__capacity_latest) = subq_58.listing__capacity_latest
GROUP BY
  COALESCE(cm_8_cte.listing__capacity_latest, subq_48.listing__capacity_latest, subq_58.listing__capacity_latest)
