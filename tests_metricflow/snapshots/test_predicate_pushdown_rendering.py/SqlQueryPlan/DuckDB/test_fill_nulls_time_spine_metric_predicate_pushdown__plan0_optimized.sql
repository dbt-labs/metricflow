test_name: test_fill_nulls_time_spine_metric_predicate_pushdown
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a metric with a time spine and fill_nulls_with enabled.

      TODO: support metric time filters
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , listing__country_latest
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_46.metric_time__day, subq_64.metric_time__day) AS metric_time__day
    , COALESCE(subq_46.listing__country_latest, subq_64.listing__country_latest) AS listing__country_latest
    , COALESCE(MAX(subq_46.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , COALESCE(MAX(subq_64.bookings_2_weeks_ago), 0) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        time_spine_src_28006.ds AS metric_time__day
        , subq_41.listing__country_latest AS listing__country_latest
        , subq_41.bookings AS bookings
      FROM ***************************.mf_time_spine time_spine_src_28006
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
            listings_latest_src_28000.country AS listing__country_latest
            , subq_34.metric_time__day AS metric_time__day
            , subq_34.booking__is_instant AS booking__is_instant
            , subq_34.bookings AS bookings
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            -- Metric Time Dimension 'ds'
            SELECT
              DATE_TRUNC('day', ds) AS metric_time__day
              , listing_id AS listing
              , is_instant AS booking__is_instant
              , 1 AS bookings
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_34
          LEFT OUTER JOIN
            ***************************.dim_listings_latest listings_latest_src_28000
          ON
            subq_34.listing = listings_latest_src_28000.listing_id
        ) subq_38
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) subq_41
      ON
        time_spine_src_28006.ds = subq_41.metric_time__day
    ) subq_45
  ) subq_46
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , COALESCE(bookings, 0) AS bookings_2_weeks_ago
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        time_spine_src_28006.ds AS metric_time__day
        , subq_59.listing__country_latest AS listing__country_latest
        , subq_59.bookings AS bookings
      FROM ***************************.mf_time_spine time_spine_src_28006
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
            listings_latest_src_28000.country AS listing__country_latest
            , subq_52.metric_time__day AS metric_time__day
            , subq_52.booking__is_instant AS booking__is_instant
            , subq_52.bookings AS bookings
          FROM (
            -- Join to Time Spine Dataset
            SELECT
              time_spine_src_28006.ds AS metric_time__day
              , subq_48.listing AS listing
              , subq_48.booking__is_instant AS booking__is_instant
              , subq_48.bookings AS bookings
            FROM ***************************.mf_time_spine time_spine_src_28006
            INNER JOIN (
              -- Read Elements From Semantic Model 'bookings_source'
              -- Metric Time Dimension 'ds'
              SELECT
                DATE_TRUNC('day', ds) AS metric_time__day
                , listing_id AS listing
                , is_instant AS booking__is_instant
                , 1 AS bookings
              FROM ***************************.fct_bookings bookings_source_src_28000
            ) subq_48
            ON
              time_spine_src_28006.ds - INTERVAL 14 day = subq_48.metric_time__day
          ) subq_52
          LEFT OUTER JOIN
            ***************************.dim_listings_latest listings_latest_src_28000
          ON
            subq_52.listing = listings_latest_src_28000.listing_id
        ) subq_56
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) subq_59
      ON
        time_spine_src_28006.ds = subq_59.metric_time__day
    ) subq_63
  ) subq_64
  ON
    (
      subq_46.listing__country_latest = subq_64.listing__country_latest
    ) AND (
      subq_46.metric_time__day = subq_64.metric_time__day
    )
  GROUP BY
    COALESCE(subq_46.metric_time__day, subq_64.metric_time__day)
    , COALESCE(subq_46.listing__country_latest, subq_64.listing__country_latest)
) subq_65
