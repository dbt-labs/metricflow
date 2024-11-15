test_name: test_nested_offset_to_grain_metric_with_tiered_filters
test_filename: test_derived_metric_rendering.py
docstring:
  Tests that filters at different tiers are applied appropriately for derived metrics with offset to grain.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings_this_month_wtd - bookings AS bookings_offset_to_grain_twice_with_tiered_filters
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_42.metric_time__day, subq_72.metric_time__day) AS metric_time__day
    , MAX(subq_42.bookings) AS bookings
    , MAX(subq_72.bookings_this_month_wtd) AS bookings_this_month_wtd
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , DATE_TRUNC('month', ds) AS metric_time__month
        , DATE_TRUNC('quarter', ds) AS metric_time__quarter
        , listing_id AS listing
        , is_instant AS booking__is_instant
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_38
    WHERE (((listing IS NOT NULL) AND (metric_time__quarter >= '2020-01-01')) AND (metric_time__month >= '2020-01-01')) AND (booking__is_instant)
    GROUP BY
      metric_time__day
  ) subq_42
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    SELECT
      subq_69.metric_time__day AS metric_time__day
      , subq_68.bookings_this_month_wtd AS bookings_this_month_wtd
    FROM (
      -- Filter Time Spine
      SELECT
        metric_time__day
      FROM (
        -- Time Spine
        SELECT
          ds AS metric_time__day
          , DATE_TRUNC('month', ds) AS metric_time__month
          , DATE_TRUNC('quarter', ds) AS metric_time__quarter
        FROM ***************************.mf_time_spine subq_70
      ) subq_71
      WHERE (
        metric_time__quarter >= '2020-01-01'
      ) AND (
        metric_time__month >= '2020-01-01'
      )
    ) subq_69
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , bookings - bookings_at_start_of_month AS bookings_this_month_wtd
      FROM (
        -- Combine Aggregated Outputs
        SELECT
          COALESCE(subq_52.metric_time__day, subq_66.metric_time__day) AS metric_time__day
          , MAX(subq_52.bookings) AS bookings
          , MAX(subq_66.bookings_at_start_of_month) AS bookings_at_start_of_month
        FROM (
          -- Constrain Output with WHERE
          -- Pass Only Elements: ['bookings', 'metric_time__day']
          -- Aggregate Measures
          -- Compute Metrics via Expressions
          SELECT
            metric_time__day
            , SUM(bookings) AS bookings
          FROM (
            -- Join Standard Outputs
            SELECT
              DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
              , subq_44.booking__ds__year AS booking__ds__year
              , subq_44.metric_time__day AS metric_time__day
              , subq_44.listing AS listing
              , subq_44.booking__is_instant AS booking__is_instant
              , subq_44.bookings AS bookings
            FROM (
              -- Read Elements From Semantic Model 'bookings_source'
              -- Metric Time Dimension 'ds'
              SELECT
                DATE_TRUNC('year', ds) AS booking__ds__year
                , DATE_TRUNC('day', ds) AS metric_time__day
                , listing_id AS listing
                , is_instant AS booking__is_instant
                , 1 AS bookings
              FROM ***************************.fct_bookings bookings_source_src_28000
            ) subq_44
            LEFT OUTER JOIN
              ***************************.dim_listings_latest listings_latest_src_28000
            ON
              subq_44.listing = listings_latest_src_28000.listing_id
          ) subq_48
          WHERE (((listing IS NOT NULL) AND (booking__is_instant)) AND (booking__ds__year >= '2019-01-01')) AND (listing__created_at__day >= '2020-01-02')
          GROUP BY
            metric_time__day
        ) subq_52
        FULL OUTER JOIN (
          -- Constrain Output with WHERE
          -- Pass Only Elements: ['bookings', 'metric_time__day']
          -- Aggregate Measures
          -- Compute Metrics via Expressions
          SELECT
            metric_time__day
            , SUM(bookings) AS bookings_at_start_of_month
          FROM (
            -- Join Standard Outputs
            SELECT
              DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
              , subq_58.metric_time__day AS metric_time__day
              , subq_58.listing AS listing
              , subq_58.booking__is_instant AS booking__is_instant
              , subq_58.bookings AS bookings
            FROM (
              -- Join to Time Spine Dataset
              SELECT
                subq_55.metric_time__day AS metric_time__day
                , subq_54.listing AS listing
                , subq_54.booking__is_instant AS booking__is_instant
                , subq_54.bookings AS bookings
              FROM (
                -- Filter Time Spine
                SELECT
                  metric_time__day
                FROM (
                  -- Time Spine
                  SELECT
                    DATE_TRUNC('year', ds) AS booking__ds__year
                    , ds AS metric_time__day
                  FROM ***************************.mf_time_spine subq_56
                ) subq_57
                WHERE booking__ds__year >= '2019-01-01'
              ) subq_55
              INNER JOIN (
                -- Read Elements From Semantic Model 'bookings_source'
                -- Metric Time Dimension 'ds'
                SELECT
                  DATE_TRUNC('day', ds) AS metric_time__day
                  , listing_id AS listing
                  , is_instant AS booking__is_instant
                  , 1 AS bookings
                FROM ***************************.fct_bookings bookings_source_src_28000
              ) subq_54
              ON
                DATE_TRUNC('month', subq_55.metric_time__day) = subq_54.metric_time__day
            ) subq_58
            LEFT OUTER JOIN
              ***************************.dim_listings_latest listings_latest_src_28000
            ON
              subq_58.listing = listings_latest_src_28000.listing_id
          ) subq_62
          WHERE ((listing IS NOT NULL) AND (booking__is_instant)) AND (listing__created_at__day >= '2020-01-02')
          GROUP BY
            metric_time__day
        ) subq_66
        ON
          subq_52.metric_time__day = subq_66.metric_time__day
        GROUP BY
          COALESCE(subq_52.metric_time__day, subq_66.metric_time__day)
      ) subq_67
    ) subq_68
    ON
      DATE_TRUNC('week', subq_69.metric_time__day) = subq_68.metric_time__day
  ) subq_72
  ON
    subq_42.metric_time__day = subq_72.metric_time__day
  GROUP BY
    COALESCE(subq_42.metric_time__day, subq_72.metric_time__day)
) subq_73
