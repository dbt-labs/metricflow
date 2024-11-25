test_name: test_nested_offset_window_metric_with_tiered_filters
test_filename: test_derived_metric_rendering.py
docstring:
  Tests that filters at different tiers are applied appropriately for derived metrics.

      This includes filters at the input metric, metric, and query level. At each tier there are filters on both
      metric_time / agg time and another dimension, which might have different behaviors.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings_offset_once AS bookings_offset_twice_with_tiered_filters
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_34.metric_time__day AS metric_time__day
    , subq_33.bookings_offset_once AS bookings_offset_once
  FROM (
    -- Filter Time Spine
    SELECT
      metric_time__day
    FROM (
      -- Time Spine
      SELECT
        DATE_TRUNC('quarter', ds) AS booking__ds__quarter
        , ds AS metric_time__day
        , DATE_TRUNC('year', ds) AS metric_time__year
      FROM ***************************.mf_time_spine subq_35
    ) subq_36
    WHERE (
      booking__ds__quarter >= '2020-01-01'
    ) AND (
      metric_time__year >= '2019-01-01'
    )
  ) subq_34
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , 2 * bookings AS bookings_offset_once
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
          , subq_24.metric_time__day AS metric_time__day
          , subq_24.listing AS listing
          , subq_24.booking__is_instant AS booking__is_instant
          , subq_24.bookings AS bookings
        FROM (
          -- Join to Time Spine Dataset
          SELECT
            subq_21.metric_time__day AS metric_time__day
            , subq_20.listing AS listing
            , subq_20.booking__is_instant AS booking__is_instant
            , subq_20.bookings AS bookings
          FROM (
            -- Filter Time Spine
            SELECT
              metric_time__day
            FROM (
              -- Time Spine
              SELECT
                ds AS metric_time__day
                , DATE_TRUNC('month', ds) AS metric_time__month
              FROM ***************************.mf_time_spine subq_22
            ) subq_23
            WHERE metric_time__month >= '2019-12-01'
          ) subq_21
          INNER JOIN (
            -- Read Elements From Semantic Model 'bookings_source'
            -- Metric Time Dimension 'ds'
            SELECT
              DATE_TRUNC('day', ds) AS metric_time__day
              , listing_id AS listing
              , is_instant AS booking__is_instant
              , 1 AS bookings
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_20
          ON
            subq_21.metric_time__day - INTERVAL 5 day = subq_20.metric_time__day
        ) subq_24
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          subq_24.listing = listings_latest_src_28000.listing_id
      ) subq_28
      WHERE ((listing__created_at__day = '2020-01-01') AND (listing IS NOT NULL)) AND (booking__is_instant)
      GROUP BY
        metric_time__day
    ) subq_32
  ) subq_33
  ON
    subq_34.metric_time__day - INTERVAL 1 month = subq_33.metric_time__day
) subq_37
