test_name: test_offset_metric_with_separate_metric_time_and_dimension_filters
test_filename: test_offset_metrics_with_filters.py
docstring:
  Test querying a time-offset metric with separate filters that allow for different filter placement.
sql_engine: DuckDB
expectation_description:
  The metric_time portion of the filter (`{{ TimeDimension('metric_time', 'day')
  }} = '2020-01-01'`) should be applied on the time spine / output side of the
  offset join, ideally by pushing it to the time spine before the join, while the
  dimension portion (`{{ Dimension('listing__country_latest') }} == 'us'`) should
  stay on the pre-offset metric input.
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , 2 * bookings AS bookings_offset_once
FROM (
  -- Join to Time Spine Dataset
  -- Compute Metrics via Expressions
  SELECT
    subq_32.metric_time__day AS metric_time__day
    , subq_27.__bookings AS bookings
  FROM (
    -- Constrain Output with WHERE
    -- Select: ['metric_time__day']
    SELECT
      metric_time__day
    FROM (
      -- Read From Time Spine 'mf_time_spine'
      -- Change Column Aliases
      -- Select: ['metric_time__day']
      SELECT
        ds AS metric_time__day
      FROM ***************************.mf_time_spine time_spine_src_28006
    ) subq_30
    WHERE metric_time__day = '2020-01-01'
  ) subq_32
  INNER JOIN (
    -- Constrain Output with WHERE
    -- Select: ['__bookings', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , SUM(bookings) AS __bookings
    FROM (
      -- Join Standard Outputs
      -- Select: ['__bookings', 'listing__country_latest', 'metric_time__day']
      SELECT
        subq_19.metric_time__day AS metric_time__day
        , listings_latest_src_28000.country AS listing__country_latest
        , subq_19.__bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS __bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_19
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_19.listing = listings_latest_src_28000.listing_id
    ) subq_24
    WHERE listing__country_latest == 'us'
    GROUP BY
      metric_time__day
  ) subq_27
  ON
    subq_32.metric_time__day - INTERVAL 5 day = subq_27.metric_time__day
) subq_34
