test_name: test_saved_query_with_metric_joins_and_filter
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we join to a time spine and query the filter input.

      This should produce a SQL query that applies the filter outside of the time spine join.
sql_engine: ClickHouse
---
WITH sma_28014_cte AS (
  SELECT
    listing_id AS listing
    , is_lux AS is_lux_latest
    , capacity AS capacity_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

, cm_6_cte AS (
  SELECT
    listing__capacity_latest
    , SUM(__bookings) AS bookings
  FROM (
    SELECT
      listing__capacity_latest
      , bookings AS __bookings
    FROM (
      SELECT
        subq_27.metric_time__day AS metric_time__day
        , sma_28014_cte.is_lux_latest AS listing__is_lux_latest
        , sma_28014_cte.capacity_latest AS listing__capacity_latest
        , subq_27.__bookings AS bookings
      FROM (
        SELECT
          toStartOfDay(ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS __bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_27
      LEFT OUTER JOIN
        sma_28014_cte
      ON
        subq_27.listing = sma_28014_cte.listing
    ) subq_32
    WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
  ) subq_34
  GROUP BY
    listing__capacity_latest
)

, cm_7_cte AS (
  SELECT
    listing__capacity_latest
    , SUM(__views) AS views
  FROM (
    SELECT
      listing__capacity_latest
      , views AS __views
    FROM (
      SELECT
        subq_38.metric_time__day AS metric_time__day
        , sma_28014_cte.is_lux_latest AS listing__is_lux_latest
        , sma_28014_cte.capacity_latest AS listing__capacity_latest
        , subq_38.__views AS views
      FROM (
        SELECT
          toStartOfDay(ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS __views
        FROM ***************************.fct_views views_source_src_28000
      ) subq_38
      LEFT OUTER JOIN
        sma_28014_cte
      ON
        subq_38.listing = sma_28014_cte.listing
    ) subq_42
    WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
  ) subq_44
  GROUP BY
    listing__capacity_latest
)

SELECT
  COALESCE(cm_6_cte.listing__capacity_latest, cm_7_cte.listing__capacity_latest, subq_49.listing__capacity_latest) AS listing__capacity_latest
  , MAX(cm_6_cte.bookings) AS bookings
  , MAX(cm_7_cte.views) AS views
  , MAX(CAST(subq_49.bookings AS Nullable(Float64)) / CAST(NULLIF(subq_49.views, 0) AS Nullable(Float64))) AS bookings_per_view
FROM cm_6_cte
FULL OUTER JOIN
  cm_7_cte
ON
  cm_6_cte.listing__capacity_latest = cm_7_cte.listing__capacity_latest
FULL OUTER JOIN (
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
