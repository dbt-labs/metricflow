test_name: test_saved_query_with_metric_joins_and_filter
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we join to a time spine and query the filter input.

      This should produce a SQL query that applies the filter outside of the time spine join.
sql_engine: Clickhouse
---
-- Combine Aggregated Outputs
WITH cm_6_cte AS (
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
      listings_latest_src_28000.is_lux AS listing__is_lux_latest
      , listings_latest_src_28000.capacity AS listing__capacity_latest
      , subq_43.metric_time__day AS metric_time__day
      , subq_43.bookings AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        date_trunc('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_43
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_43.listing = listings_latest_src_28000.listing_id
  ) subq_47
  WHERE ((listing__is_lux_latest) AND (metric_time__day >= '2020-01-02'))
  GROUP BY
    listing__capacity_latest
)

, cm_7_cte AS (
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
      listings_latest_src_28000.is_lux AS listing__is_lux_latest
      , listings_latest_src_28000.capacity AS listing__capacity_latest
      , subq_53.metric_time__day AS metric_time__day
      , subq_53.views AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      SELECT
        date_trunc('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS views
      FROM ***************************.fct_views views_source_src_28000
    ) subq_53
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_53.listing = listings_latest_src_28000.listing_id
  ) subq_57
  WHERE ((listing__is_lux_latest) AND (metric_time__day >= '2020-01-02'))
  GROUP BY
    listing__capacity_latest
)

SELECT
  COALESCE(cm_6_cte.listing__capacity_latest, cm_7_cte.listing__capacity_latest, subq_64.listing__capacity_latest) AS listing__capacity_latest
  , MAX(cm_6_cte.bookings) AS bookings
  , MAX(cm_7_cte.views) AS views
  , MAX(CAST(subq_64.bookings AS Nullable(DOUBLE PRECISION)) / CAST(NULLIF(subq_64.views, 0) AS Nullable(DOUBLE PRECISION))) AS bookings_per_view
FROM cm_6_cte cm_6_cte
FULL OUTER JOIN
  cm_7_cte cm_7_cte
ON
  cm_6_cte.listing__capacity_latest = cm_7_cte.listing__capacity_latest
FULL OUTER JOIN (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(cm_6_cte.listing__capacity_latest, cm_7_cte.listing__capacity_latest) AS listing__capacity_latest
    , MAX(cm_6_cte.bookings) AS bookings
    , MAX(cm_7_cte.views) AS views
  FROM cm_6_cte cm_6_cte
  FULL OUTER JOIN
    cm_7_cte cm_7_cte
  ON
    cm_6_cte.listing__capacity_latest = cm_7_cte.listing__capacity_latest
  GROUP BY
    listing__capacity_latest
) subq_64
ON
  COALESCE(cm_6_cte.listing__capacity_latest, cm_7_cte.listing__capacity_latest) = subq_64.listing__capacity_latest
GROUP BY
  listing__capacity_latest
