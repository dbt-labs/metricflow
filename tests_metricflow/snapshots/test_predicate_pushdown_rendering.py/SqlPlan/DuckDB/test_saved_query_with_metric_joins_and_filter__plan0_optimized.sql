test_name: test_saved_query_with_metric_joins_and_filter
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we join to a time spine and query the filter input.

      This should produce a SQL query that applies the filter outside of the time spine join.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28014_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , is_lux AS is_lux_latest
    , capacity AS capacity_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

SELECT
  listing__capacity_latest AS listing__capacity_latest
  , bookings AS bookings
  , views AS views
  , CAST(bookings AS DOUBLE) / CAST(NULLIF(views, 0) AS DOUBLE) AS bookings_per_view
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_33.listing__capacity_latest, subq_43.listing__capacity_latest) AS listing__capacity_latest
    , MAX(subq_33.bookings) AS bookings
    , MAX(subq_43.views) AS views
  FROM (
    -- Constrain Output with WHERE
    -- Select: ['__bookings', 'listing__capacity_latest']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      listing__capacity_latest
      , SUM(bookings) AS bookings
    FROM (
      -- Join Standard Outputs
      -- Select: ['__bookings', 'listing__capacity_latest', 'listing__is_lux_latest', 'metric_time__day']
      SELECT
        subq_24.metric_time__day AS metric_time__day
        , sma_28014_cte.is_lux_latest AS listing__is_lux_latest
        , sma_28014_cte.capacity_latest AS listing__capacity_latest
        , subq_24.__bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS __bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_24
      LEFT OUTER JOIN
        sma_28014_cte
      ON
        subq_24.listing = sma_28014_cte.listing
    ) subq_29
    WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
    GROUP BY
      listing__capacity_latest
  ) subq_33
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Select: ['__views', 'listing__capacity_latest']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      listing__capacity_latest
      , SUM(views) AS views
    FROM (
      -- Join Standard Outputs
      -- Select: ['__views', 'listing__capacity_latest', 'listing__is_lux_latest', 'metric_time__day']
      SELECT
        subq_35.metric_time__day AS metric_time__day
        , sma_28014_cte.is_lux_latest AS listing__is_lux_latest
        , sma_28014_cte.capacity_latest AS listing__capacity_latest
        , subq_35.__views AS views
      FROM (
        -- Read Elements From Semantic Model 'views_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS __views
        FROM ***************************.fct_views views_source_src_28000
      ) subq_35
      LEFT OUTER JOIN
        sma_28014_cte
      ON
        subq_35.listing = sma_28014_cte.listing
    ) subq_39
    WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
    GROUP BY
      listing__capacity_latest
  ) subq_43
  ON
    subq_33.listing__capacity_latest = subq_43.listing__capacity_latest
  GROUP BY
    COALESCE(subq_33.listing__capacity_latest, subq_43.listing__capacity_latest)
) subq_44
