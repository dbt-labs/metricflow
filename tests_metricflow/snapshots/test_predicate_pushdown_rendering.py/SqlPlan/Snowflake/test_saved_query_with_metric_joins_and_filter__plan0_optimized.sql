test_name: test_saved_query_with_metric_joins_and_filter
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we join to a time spine and query the filter input.

      This should produce a SQL query that applies the filter outside of the time spine join.
sql_engine: Snowflake
---
-- Combine Aggregated Outputs
-- Write to DataTable
WITH cm_6_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['__bookings', 'listing__capacity_latest']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    listing__capacity_latest
    , SUM(bookings) AS bookings
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['__bookings', 'listing__capacity_latest', 'listing__is_lux_latest', 'metric_time__day']
    SELECT
      subq_27.metric_time__day AS metric_time__day
      , listings_latest_src_28000.is_lux AS listing__is_lux_latest
      , listings_latest_src_28000.capacity AS listing__capacity_latest
      , subq_27.__bookings AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_27
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_27.listing = listings_latest_src_28000.listing_id
  ) subq_32
  WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
  GROUP BY
    listing__capacity_latest
)

, cm_7_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['__views', 'listing__capacity_latest']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    listing__capacity_latest
    , SUM(views) AS views
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['__views', 'listing__capacity_latest', 'listing__is_lux_latest', 'metric_time__day']
    SELECT
      subq_38.metric_time__day AS metric_time__day
      , listings_latest_src_28000.is_lux AS listing__is_lux_latest
      , listings_latest_src_28000.capacity AS listing__capacity_latest
      , subq_38.__views AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS __views
      FROM ***************************.fct_views views_source_src_28000
    ) subq_38
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_38.listing = listings_latest_src_28000.listing_id
  ) subq_42
  WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
  GROUP BY
    listing__capacity_latest
)

SELECT
  COALESCE(cm_6_cte.listing__capacity_latest, cm_7_cte.listing__capacity_latest, subq_49.listing__capacity_latest) AS listing__capacity_latest
  , MAX(cm_6_cte.bookings) AS bookings
  , MAX(cm_7_cte.views) AS views
  , MAX(CAST(subq_49.bookings AS DOUBLE) / CAST(NULLIF(subq_49.views, 0) AS DOUBLE)) AS bookings_per_view
FROM cm_6_cte
FULL OUTER JOIN
  cm_7_cte
ON
  cm_6_cte.listing__capacity_latest = cm_7_cte.listing__capacity_latest
FULL OUTER JOIN (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(cm_6_cte.listing__capacity_latest, cm_7_cte.listing__capacity_latest) AS listing__capacity_latest
    , MAX(cm_6_cte.bookings) AS bookings
    , MAX(cm_7_cte.views) AS views
  FROM cm_6_cte
  FULL OUTER JOIN
    cm_7_cte
  ON
    cm_6_cte.listing__capacity_latest = cm_7_cte.listing__capacity_latest
  GROUP BY
    COALESCE(cm_6_cte.listing__capacity_latest, cm_7_cte.listing__capacity_latest)
) subq_49
ON
  COALESCE(cm_6_cte.listing__capacity_latest, cm_7_cte.listing__capacity_latest) = subq_49.listing__capacity_latest
GROUP BY
  COALESCE(cm_6_cte.listing__capacity_latest, cm_7_cte.listing__capacity_latest, subq_49.listing__capacity_latest)
