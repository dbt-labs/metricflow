test_name: test_nested_offset_metric_with_non_queried_element_in_filter
test_filename: test_offset_metrics_with_filters.py
docstring:
  Tests that a non-queried filter element does not remain in the aggregation grain.
sql_engine: DuckDB
expectation_description:
  The non-queried listing filter should be available for filtering the nested
  offset metric, but it should not remain part of the aggregation grain after
  filtering. The current snapshot does not reflect the correct result.
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH rss_28018_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS ds__day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Constrain Output with WHERE
  -- Select: ['metric_time__day', 'bookings_offset_once']
  SELECT
    metric_time__day
    , bookings_offset_once
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_31.listing AS listing
      , subq_31.bookings_offset_once AS bookings_offset_once
    FROM rss_28018_cte
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , listing
        , 2 * bookings AS bookings_offset_once
      FROM (
        -- Join to Time Spine Dataset
        -- Compute Metrics via Expressions
        SELECT
          rss_28018_cte.ds__day AS metric_time__day
          , subq_24.listing AS listing
          , subq_24.__bookings AS bookings
        FROM rss_28018_cte
        INNER JOIN (
          -- Aggregate Inputs for Simple Metrics
          SELECT
            metric_time__day
            , listing
            , SUM(__bookings) AS __bookings
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            -- Metric Time Dimension 'ds'
            -- Select: ['__bookings', 'metric_time__day', 'listing']
            -- Select: ['__bookings', 'metric_time__day', 'listing']
            SELECT
              DATE_TRUNC('day', ds) AS metric_time__day
              , listing_id AS listing
              , 1 AS __bookings
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_23
          GROUP BY
            metric_time__day
            , listing
        ) subq_24
        ON
          rss_28018_cte.ds__day - INTERVAL 5 day = subq_24.metric_time__day
      ) subq_30
    ) subq_31
    ON
      rss_28018_cte.ds__day - INTERVAL 2 day = subq_31.metric_time__day
  ) subq_36
  WHERE listing = '1'
) subq_38
