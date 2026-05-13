test_name: test_offset_metric_with_query_time_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a query against a derived offset metric.

      TODO: support metric time filters
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , listing_id AS listing
    , is_instant AS booking__is_instant
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

, sma_28014_cte AS (
  SELECT
    listing_id AS listing
    , country AS country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , listing__country_latest AS listing__country_latest
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  SELECT
    COALESCE(subq_37.metric_time__day, subq_51.metric_time__day) AS metric_time__day
    , COALESCE(subq_37.listing__country_latest, subq_51.listing__country_latest) AS listing__country_latest
    , MAX(subq_37.bookings) AS bookings
    , MAX(subq_51.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    SELECT
      metric_time__day
      , listing__country_latest
      , SUM(__bookings) AS bookings
    FROM (
      SELECT
        metric_time__day
        , listing__country_latest
        , bookings AS __bookings
      FROM (
        SELECT
          sma_28009_cte.metric_time__day AS metric_time__day
          , sma_28009_cte.booking__is_instant AS booking__is_instant
          , sma_28014_cte.country_latest AS listing__country_latest
          , sma_28009_cte.__bookings AS bookings
        FROM sma_28009_cte
        LEFT OUTER JOIN
          sma_28014_cte
        ON
          sma_28009_cte.listing = sma_28014_cte.listing
      ) subq_33
      WHERE booking__is_instant
    ) subq_35
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) subq_37
  FULL OUTER JOIN (
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , subq_45.listing__country_latest AS listing__country_latest
      , subq_45.__bookings AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      SELECT
        metric_time__day
        , listing__country_latest
        , SUM(__bookings) AS __bookings
      FROM (
        SELECT
          metric_time__day
          , listing__country_latest
          , bookings AS __bookings
        FROM (
          SELECT
            sma_28009_cte.metric_time__day AS metric_time__day
            , sma_28009_cte.booking__is_instant AS booking__is_instant
            , sma_28014_cte.country_latest AS listing__country_latest
            , sma_28009_cte.__bookings AS bookings
          FROM sma_28009_cte
          LEFT OUTER JOIN
            sma_28014_cte
          ON
            sma_28009_cte.listing = sma_28014_cte.listing
        ) subq_42
        WHERE booking__is_instant
      ) subq_44
      GROUP BY
        metric_time__day
        , listing__country_latest
    ) subq_45
    ON
      addDays(time_spine_src_28006.ds, -14) = subq_45.metric_time__day
  ) subq_51
  ON
    (
      subq_37.listing__country_latest = subq_51.listing__country_latest
    ) AND (
      subq_37.metric_time__day = subq_51.metric_time__day
    )
  GROUP BY
    COALESCE(subq_37.metric_time__day, subq_51.metric_time__day)
    , COALESCE(subq_37.listing__country_latest, subq_51.listing__country_latest)
) subq_52
