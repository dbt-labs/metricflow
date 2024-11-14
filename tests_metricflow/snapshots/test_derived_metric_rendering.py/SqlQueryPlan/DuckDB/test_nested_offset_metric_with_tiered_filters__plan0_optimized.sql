test_name: test_nested_offset_metric_with_tiered_filters
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
  , bookings AS bookings_offset_twice_with_tiered_filters
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__day', 'bookings_offset_once']
  SELECT
    metric_time__day
    , bookings_offset_once
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_32.ds AS metric_time__day
      , DATE_TRUNC('month', subq_32.ds) AS metric_time__month
      , DATE_TRUNC('year', subq_32.ds) AS metric_time__year
      , subq_30.booking__ds__quarter AS booking__ds__quarter
      , subq_30.listing__created_at__day AS listing__created_at__day
      , subq_30.listing AS listing
      , subq_30.booking__is_instant AS booking__is_instant
      , subq_30.bookings_offset_once AS bookings_offset_once
    FROM ***************************.mf_time_spine subq_32
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , booking__ds__quarter
        , listing__created_at__day
        , listing
        , booking__is_instant
        , 2 * bookings AS bookings_offset_once
      FROM (
        -- Join Standard Outputs
        -- Pass Only Elements: [
        --   'bookings',
        --   'booking__is_instant',
        --   'metric_time__day',
        --   'metric_time__year',
        --   'metric_time__month',
        --   'booking__ds__quarter',
        --   'listing__created_at__day',
        --   'listing',
        -- ]
        -- Aggregate Measures
        -- Compute Metrics via Expressions
        SELECT
          subq_22.metric_time__day AS metric_time__day
          , subq_22.metric_time__month AS metric_time__month
          , subq_22.metric_time__year AS metric_time__year
          , subq_22.booking__ds__quarter AS booking__ds__quarter
          , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
          , subq_22.listing AS listing
          , subq_22.booking__is_instant AS booking__is_instant
          , SUM(subq_22.bookings) AS bookings
        FROM (
          -- Join to Time Spine Dataset
          SELECT
            subq_21.ds AS metric_time__day
            , DATE_TRUNC('month', subq_21.ds) AS metric_time__month
            , DATE_TRUNC('year', subq_21.ds) AS metric_time__year
            , subq_19.booking__ds__quarter AS booking__ds__quarter
            , subq_19.listing AS listing
            , subq_19.booking__is_instant AS booking__is_instant
            , subq_19.bookings AS bookings
          FROM ***************************.mf_time_spine subq_21
          INNER JOIN (
            -- Read Elements From Semantic Model 'bookings_source'
            -- Metric Time Dimension 'ds'
            SELECT
              DATE_TRUNC('quarter', ds) AS booking__ds__quarter
              , DATE_TRUNC('day', ds) AS metric_time__day
              , listing_id AS listing
              , is_instant AS booking__is_instant
              , 1 AS bookings
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_19
          ON
            subq_21.ds - INTERVAL 5 day = subq_19.metric_time__day
        ) subq_22
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          subq_22.listing = listings_latest_src_28000.listing_id
        GROUP BY
          subq_22.metric_time__day
          , subq_22.metric_time__month
          , subq_22.metric_time__year
          , subq_22.booking__ds__quarter
          , DATE_TRUNC('day', listings_latest_src_28000.created_at)
          , subq_22.listing
          , subq_22.booking__is_instant
      ) subq_29
    ) subq_30
    ON
      subq_32.ds - INTERVAL 1 month = subq_30.metric_time__day
  ) subq_33
  WHERE (((((metric_time__year >= '2020-01-01') AND (listing IS NOT NULL)) AND (metric_time__month >= '2019-01-01')) AND (booking__is_instant)) AND (booking__ds__quarter = '2021-01-01')) AND (listing__created_at__day = '2021-01-01')
) subq_35
