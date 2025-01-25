test_name: test_fill_nulls_time_spine_metric_predicate_pushdown
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a metric with a time spine and fill_nulls_with enabled.

      TODO: support metric time filters
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , listing__country_latest
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_40.metric_time__day, nr_subq_56.metric_time__day) AS metric_time__day
    , COALESCE(nr_subq_40.listing__country_latest, nr_subq_56.listing__country_latest) AS listing__country_latest
    , COALESCE(MAX(nr_subq_40.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , COALESCE(MAX(nr_subq_56.bookings_2_weeks_ago), 0) AS bookings_2_weeks_ago
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
        , nr_subq_35.listing__country_latest AS listing__country_latest
        , nr_subq_35.bookings AS bookings
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
            , nr_subq_29.metric_time__day AS metric_time__day
            , nr_subq_29.booking__is_instant AS booking__is_instant
            , nr_subq_29.bookings AS bookings
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            -- Metric Time Dimension 'ds'
            SELECT
              DATE_TRUNC('day', ds) AS metric_time__day
              , listing_id AS listing
              , is_instant AS booking__is_instant
              , 1 AS bookings
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) nr_subq_29
          LEFT OUTER JOIN
            ***************************.dim_listings_latest listings_latest_src_28000
          ON
            nr_subq_29.listing = listings_latest_src_28000.listing_id
        ) nr_subq_32
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) nr_subq_35
      ON
        time_spine_src_28006.ds = nr_subq_35.metric_time__day
    ) nr_subq_39
  ) nr_subq_40
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
        , nr_subq_51.listing__country_latest AS listing__country_latest
        , nr_subq_51.bookings AS bookings
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
            , nr_subq_45.metric_time__day AS metric_time__day
            , nr_subq_45.booking__is_instant AS booking__is_instant
            , nr_subq_45.bookings AS bookings
          FROM (
            -- Join to Time Spine Dataset
            SELECT
              time_spine_src_28006.ds AS metric_time__day
              , nr_subq_41.listing AS listing
              , nr_subq_41.booking__is_instant AS booking__is_instant
              , nr_subq_41.bookings AS bookings
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
            ) nr_subq_41
            ON
              DATEADD(day, -14, time_spine_src_28006.ds) = nr_subq_41.metric_time__day
          ) nr_subq_45
          LEFT OUTER JOIN
            ***************************.dim_listings_latest listings_latest_src_28000
          ON
            nr_subq_45.listing = listings_latest_src_28000.listing_id
        ) nr_subq_48
        WHERE booking__is_instant
        GROUP BY
          metric_time__day
          , listing__country_latest
      ) nr_subq_51
      ON
        time_spine_src_28006.ds = nr_subq_51.metric_time__day
    ) nr_subq_55
  ) nr_subq_56
  ON
    (
      nr_subq_40.listing__country_latest = nr_subq_56.listing__country_latest
    ) AND (
      nr_subq_40.metric_time__day = nr_subq_56.metric_time__day
    )
  GROUP BY
    COALESCE(nr_subq_40.metric_time__day, nr_subq_56.metric_time__day)
    , COALESCE(nr_subq_40.listing__country_latest, nr_subq_56.listing__country_latest)
) nr_subq_57
