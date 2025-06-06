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
    , 1 AS bookings
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
    COALESCE(subq_45.metric_time__day, subq_61.metric_time__day) AS metric_time__day
    , COALESCE(subq_45.listing__country_latest, subq_61.listing__country_latest) AS listing__country_latest
    , COALESCE(MAX(subq_45.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , COALESCE(MAX(subq_61.bookings_2_weeks_ago), 0) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        rss_28018_cte.ds__day AS metric_time__day
        , subq_40.listing__country_latest AS listing__country_latest
        , subq_40.bookings AS bookings
      FROM rss_28018_cte
      LEFT OUTER JOIN (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
        -- Aggregate Measures
        SELECT
          metric_time__day
          , listing__country_latest
          , SUM(bookings) AS bookings
        FROM (
          -- Join Standard Outputs
          SELECT
            sma_28014_cte.country_latest AS listing__country_latest
            , sma_28009_cte.metric_time__day AS metric_time__day
            , sma_28009_cte.booking__is_instant AS booking__is_instant
            , sma_28009_cte.bookings AS bookings
          FROM sma_28009_cte
          LEFT OUTER JOIN
            sma_28014_cte
          ON
            sma_28009_cte.listing = sma_28014_cte.listing
        ) subq_37
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) subq_40
      ON
        rss_28018_cte.ds__day = subq_40.metric_time__day
    ) subq_44
  ) subq_45
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , COALESCE(bookings, 0) AS bookings_2_weeks_ago
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        rss_28018_cte.ds__day AS metric_time__day
        , subq_56.listing__country_latest AS listing__country_latest
        , subq_56.bookings AS bookings
      FROM rss_28018_cte
      LEFT OUTER JOIN (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
        -- Aggregate Measures
        SELECT
          metric_time__day
          , listing__country_latest
          , SUM(bookings) AS bookings
        FROM (
          -- Join Standard Outputs
          SELECT
            sma_28014_cte.country_latest AS listing__country_latest
            , subq_50.metric_time__day AS metric_time__day
            , subq_50.booking__is_instant AS booking__is_instant
            , subq_50.bookings AS bookings
          FROM (
            -- Join to Time Spine Dataset
            SELECT
              rss_28018_cte.ds__day AS metric_time__day
              , sma_28009_cte.listing AS listing
              , sma_28009_cte.booking__is_instant AS booking__is_instant
              , sma_28009_cte.bookings AS bookings
            FROM rss_28018_cte
            INNER JOIN
              sma_28009_cte
            ON
              rss_28018_cte.ds__day - INTERVAL 14 day = sma_28009_cte.metric_time__day
          ) subq_50
          LEFT OUTER JOIN
            sma_28014_cte
          ON
            subq_50.listing = sma_28014_cte.listing
        ) subq_53
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) subq_56
      ON
        rss_28018_cte.ds__day = subq_56.metric_time__day
    ) subq_60
  ) subq_61
  ON
    (
      subq_45.listing__country_latest = subq_61.listing__country_latest
    ) AND (
      subq_45.metric_time__day = subq_61.metric_time__day
    )
  GROUP BY
    COALESCE(subq_45.metric_time__day, subq_61.metric_time__day)
    , COALESCE(subq_45.listing__country_latest, subq_61.listing__country_latest)
) subq_62
