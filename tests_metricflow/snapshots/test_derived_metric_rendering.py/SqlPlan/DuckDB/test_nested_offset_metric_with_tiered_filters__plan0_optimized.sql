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
      , subq_35.bookings_offset_once AS bookings_offset_once
    FROM rss_28018_cte rss_28018_cte
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
            , subq_26.metric_time__day AS metric_time__day
            , subq_26.listing AS listing
            , subq_26.booking__is_instant AS booking__is_instant
            , subq_26.bookings AS bookings
          FROM (
            -- Join to Time Spine Dataset
            SELECT
              rss_28018_cte.ds__day AS metric_time__day
              , subq_22.listing AS listing
              , subq_22.booking__is_instant AS booking__is_instant
              , subq_22.bookings AS bookings
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
            ) subq_22
            ON
              rss_28018_cte.ds__day - INTERVAL 5 day = subq_22.metric_time__day
          ) subq_26
          LEFT OUTER JOIN
            ***************************.dim_listings_latest listings_latest_src_28000
          ON
            subq_26.listing = listings_latest_src_28000.listing_id
        ) subq_30
        WHERE ((listing IS NOT NULL) AND (booking__is_instant)) AND (listing__created_at__day = '2021-01-01')
        GROUP BY
          metric_time__day
      ) subq_34
    ) subq_35
    ON
      rss_28018_cte.ds__day - INTERVAL 1 month = subq_35.metric_time__day
  ) subq_39
  WHERE ((metric_time__year >= '2020-01-01') AND (metric_time__month >= '2019-01-01')) AND (booking__ds__quarter = '2021-01-01')
) subq_41
