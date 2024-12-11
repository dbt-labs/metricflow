test_name: test_fill_nulls_time_spine_metric_predicate_pushdown
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a metric with a time spine and fill_nulls_with enabled.

      TODO: support metric time filters
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
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

SELECT
  metric_time__day AS metric_time__day
  , listing__country_latest AS listing__country_latest
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_42.metric_time__day, subq_56.metric_time__day) AS metric_time__day
    , COALESCE(subq_42.listing__country_latest, subq_56.listing__country_latest) AS listing__country_latest
    , COALESCE(MAX(subq_42.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , COALESCE(MAX(subq_56.bookings_2_weeks_ago), 0) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_40.ds AS metric_time__day
        , subq_38.listing__country_latest AS listing__country_latest
        , subq_38.bookings AS bookings
      FROM ***************************.mf_time_spine subq_40
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
          FROM sma_28009_cte sma_28009_cte
          LEFT OUTER JOIN
            sma_28014_cte sma_28014_cte
          ON
            sma_28009_cte.listing = sma_28014_cte.listing
        ) subq_35
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) subq_38
      ON
        subq_40.ds = subq_38.metric_time__day
    ) subq_41
  ) subq_42
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , COALESCE(bookings, 0) AS bookings_2_weeks_ago
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_54.ds AS metric_time__day
        , subq_52.listing__country_latest AS listing__country_latest
        , subq_52.bookings AS bookings
      FROM ***************************.mf_time_spine subq_54
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
            , subq_46.metric_time__day AS metric_time__day
            , subq_46.booking__is_instant AS booking__is_instant
            , subq_46.bookings AS bookings
          FROM (
            -- Join to Time Spine Dataset
            SELECT
              subq_45.ds AS metric_time__day
              , sma_28009_cte.listing AS listing
              , sma_28009_cte.booking__is_instant AS booking__is_instant
              , sma_28009_cte.bookings AS bookings
            FROM ***************************.mf_time_spine subq_45
            INNER JOIN
              sma_28009_cte sma_28009_cte
            ON
              subq_45.ds - INTERVAL 14 day = sma_28009_cte.metric_time__day
          ) subq_46
          LEFT OUTER JOIN
            sma_28014_cte sma_28014_cte
          ON
            subq_46.listing = sma_28014_cte.listing
        ) subq_49
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) subq_52
      ON
        subq_54.ds = subq_52.metric_time__day
    ) subq_55
  ) subq_56
  ON
    (
      subq_42.listing__country_latest = subq_56.listing__country_latest
    ) AND (
      subq_42.metric_time__day = subq_56.metric_time__day
    )
  GROUP BY
    COALESCE(subq_42.metric_time__day, subq_56.metric_time__day)
    , COALESCE(subq_42.listing__country_latest, subq_56.listing__country_latest)
) subq_57
