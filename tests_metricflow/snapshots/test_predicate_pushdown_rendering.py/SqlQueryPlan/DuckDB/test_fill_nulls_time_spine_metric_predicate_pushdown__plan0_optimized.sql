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
    COALESCE(subq_42.metric_time__day, subq_58.metric_time__day) AS metric_time__day
    , COALESCE(subq_42.listing__country_latest, subq_58.listing__country_latest) AS listing__country_latest
    , COALESCE(MAX(subq_42.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , COALESCE(MAX(subq_58.bookings_2_weeks_ago), 0) AS bookings_2_weeks_ago
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
            listings_latest_src_28000.country AS listing__country_latest
            , subq_31.metric_time__day AS metric_time__day
            , subq_31.booking__is_instant AS booking__is_instant
            , subq_31.bookings AS bookings
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            -- Metric Time Dimension 'ds'
            SELECT
              DATE_TRUNC('day', ds) AS metric_time__day
              , listing_id AS listing
              , is_instant AS booking__is_instant
              , 1 AS bookings
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_31
          LEFT OUTER JOIN
            ***************************.dim_listings_latest listings_latest_src_28000
          ON
            subq_31.listing = listings_latest_src_28000.listing_id
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
        subq_56.ds AS metric_time__day
        , subq_54.listing__country_latest AS listing__country_latest
        , subq_54.bookings AS bookings
      FROM ***************************.mf_time_spine subq_56
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
            , subq_47.metric_time__day AS metric_time__day
            , subq_47.booking__is_instant AS booking__is_instant
            , subq_47.bookings AS bookings
          FROM (
            -- Join to Time Spine Dataset
            SELECT
              subq_46.ds AS metric_time__day
              , subq_44.listing AS listing
              , subq_44.booking__is_instant AS booking__is_instant
              , subq_44.bookings AS bookings
            FROM ***************************.mf_time_spine subq_46
            INNER JOIN (
              -- Read Elements From Semantic Model 'bookings_source'
              -- Metric Time Dimension 'ds'
              SELECT
                DATE_TRUNC('day', ds) AS metric_time__day
                , listing_id AS listing
                , is_instant AS booking__is_instant
                , 1 AS bookings
              FROM ***************************.fct_bookings bookings_source_src_28000
            ) subq_44
            ON
              subq_46.ds - INTERVAL 14 day = subq_44.metric_time__day
          ) subq_47
          LEFT OUTER JOIN
            ***************************.dim_listings_latest listings_latest_src_28000
          ON
            subq_47.listing = listings_latest_src_28000.listing_id
        ) subq_51
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) subq_54
      ON
        subq_56.ds = subq_54.metric_time__day
    ) subq_57
  ) subq_58
  ON
    (
      subq_42.listing__country_latest = subq_58.listing__country_latest
    ) AND (
      subq_42.metric_time__day = subq_58.metric_time__day
    )
  GROUP BY
    COALESCE(subq_42.metric_time__day, subq_58.metric_time__day)
    , COALESCE(subq_42.listing__country_latest, subq_58.listing__country_latest)
) subq_59
