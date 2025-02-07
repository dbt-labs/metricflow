test_name: test_nested_offset_metric_with_tiered_filters
test_filename: test_derived_metric_rendering.py
docstring:
  Tests that filters at different tiers are applied appropriately for derived metrics.

      This includes filters at the input metric, metric, and query level. At each tier there are filters on both
      metric_time / agg time and another dimension, which might have different behaviors.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
WITH rss_28018_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS ds__day
    , DATE_TRUNC('month', ds) AS ds__month
    , DATE_TRUNC('quarter', ds) AS ds__quarter
    , DATE_TRUNC('year', ds) AS ds__year
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , bookings_offset_once AS bookings_offset_twice_with_tiered_filters
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__day', 'bookings_offset_once']
  SELECT
    metric_time__day
    , bookings_offset_once
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_33.metric_time__month AS metric_time__month
      , subq_33.booking__ds__quarter AS booking__ds__quarter
      , subq_33.metric_time__year AS metric_time__year
      , subq_33.listing__created_at__day AS listing__created_at__day
      , subq_33.listing AS listing
      , subq_33.booking__is_instant AS booking__is_instant
      , subq_33.bookings_offset_once AS bookings_offset_once
    FROM rss_28018_cte rss_28018_cte
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , metric_time__month
        , booking__ds__quarter
        , metric_time__year
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
          subq_25.metric_time__day AS metric_time__day
          , subq_25.metric_time__month AS metric_time__month
          , subq_25.booking__ds__quarter AS booking__ds__quarter
          , subq_25.metric_time__year AS metric_time__year
          , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
          , subq_25.listing AS listing
          , subq_25.booking__is_instant AS booking__is_instant
          , SUM(subq_25.bookings) AS bookings
        FROM (
          -- Join to Time Spine Dataset
          SELECT
            rss_28018_cte.ds__day AS metric_time__day
            , rss_28018_cte.ds__month AS metric_time__month
            , rss_28018_cte.ds__quarter AS booking__ds__quarter
            , rss_28018_cte.ds__year AS metric_time__year
            , subq_21.listing AS listing
            , subq_21.booking__is_instant AS booking__is_instant
            , subq_21.bookings AS bookings
          FROM rss_28018_cte rss_28018_cte
          INNER JOIN (
            -- Read Elements From Semantic Model 'bookings_source'
            -- Metric Time Dimension 'ds'
            SELECT
              DATE_TRUNC('day', ds) AS metric_time__day
              , listing_id AS listing
              , is_instant AS booking__is_instant
              , 1 AS bookings
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_21
          ON
            rss_28018_cte.ds__day - INTERVAL 5 day = subq_21.metric_time__day
        ) subq_25
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          subq_25.listing = listings_latest_src_28000.listing_id
        GROUP BY
          subq_25.metric_time__day
          , subq_25.metric_time__month
          , subq_25.booking__ds__quarter
          , subq_25.metric_time__year
          , DATE_TRUNC('day', listings_latest_src_28000.created_at)
          , subq_25.listing
          , subq_25.booking__is_instant
      ) subq_32
    ) subq_33
    ON
      rss_28018_cte.ds__day - INTERVAL 1 month = subq_33.metric_time__day
  ) subq_37
  WHERE (((((metric_time__year >= '2020-01-01') AND (listing IS NOT NULL)) AND (metric_time__month >= '2019-01-01')) AND (booking__is_instant)) AND (booking__ds__quarter = '2021-01-01')) AND (listing__created_at__day = '2021-01-01')
) subq_39
