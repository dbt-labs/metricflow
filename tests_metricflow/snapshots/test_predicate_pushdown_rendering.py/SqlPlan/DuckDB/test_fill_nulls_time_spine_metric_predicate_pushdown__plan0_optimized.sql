test_name: test_fill_nulls_time_spine_metric_predicate_pushdown
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a metric with a time spine and fill_nulls_with enabled.

      TODO: support metric time filters
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , is_instant AS booking__is_instant
    , 1 AS __bookings_fill_nulls_with_0
  FROM ***************************.fct_bookings bookings_source_src_28000
)

, sma_28014_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , country AS country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

, rss_28018_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS ds__day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , listing__country_latest AS listing__country_latest
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_47.metric_time__day, subq_61.metric_time__day) AS metric_time__day
    , COALESCE(subq_47.listing__country_latest, subq_61.listing__country_latest) AS listing__country_latest
    , COALESCE(MAX(subq_47.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , COALESCE(MAX(subq_61.bookings_2_weeks_ago), 0) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        rss_28018_cte.ds__day AS metric_time__day
        , subq_41.listing__country_latest AS listing__country_latest
        , subq_41.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
      FROM rss_28018_cte
      LEFT OUTER JOIN (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['__bookings_fill_nulls_with_0', 'listing__country_latest', 'metric_time__day']
        -- Aggregate Inputs for Simple Metrics
        SELECT
          metric_time__day
          , listing__country_latest
          , SUM(bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
        FROM (
          -- Join Standard Outputs
          -- Pass Only Elements: ['__bookings_fill_nulls_with_0', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
          SELECT
            sma_28009_cte.metric_time__day AS metric_time__day
            , sma_28009_cte.booking__is_instant AS booking__is_instant
            , sma_28014_cte.country_latest AS listing__country_latest
            , sma_28009_cte.__bookings_fill_nulls_with_0 AS bookings_fill_nulls_with_0
          FROM sma_28009_cte
          LEFT OUTER JOIN
            sma_28014_cte
          ON
            sma_28009_cte.listing = sma_28014_cte.listing
        ) subq_38
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) subq_41
      ON
        rss_28018_cte.ds__day = subq_41.metric_time__day
    ) subq_46
  ) subq_47
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_2_weeks_ago
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        rss_28018_cte.ds__day AS metric_time__day
        , subq_55.listing__country_latest AS listing__country_latest
        , subq_55.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
      FROM rss_28018_cte
      LEFT OUTER JOIN (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['__bookings_fill_nulls_with_0', 'listing__country_latest', 'metric_time__day']
        -- Aggregate Inputs for Simple Metrics
        SELECT
          metric_time__day
          , listing__country_latest
          , SUM(bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
        FROM (
          -- Join Standard Outputs
          -- Pass Only Elements: ['__bookings_fill_nulls_with_0', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
          SELECT
            sma_28009_cte.metric_time__day AS metric_time__day
            , sma_28009_cte.booking__is_instant AS booking__is_instant
            , sma_28014_cte.country_latest AS listing__country_latest
            , sma_28009_cte.__bookings_fill_nulls_with_0 AS bookings_fill_nulls_with_0
          FROM sma_28009_cte
          LEFT OUTER JOIN
            sma_28014_cte
          ON
            sma_28009_cte.listing = sma_28014_cte.listing
        ) subq_52
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) subq_55
      ON
        rss_28018_cte.ds__day - INTERVAL 14 day = subq_55.metric_time__day
    ) subq_60
  ) subq_61
  ON
    (
      subq_47.listing__country_latest = subq_61.listing__country_latest
    ) AND (
      subq_47.metric_time__day = subq_61.metric_time__day
    )
  GROUP BY
    COALESCE(subq_47.metric_time__day, subq_61.metric_time__day)
    , COALESCE(subq_47.listing__country_latest, subq_61.listing__country_latest)
) subq_62
