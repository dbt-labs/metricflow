test_name: test_offset_metric_with_dimension_filter
test_filename: test_offset_metrics_with_filters.py
sql_engine: DuckDB
expectation_description:
  The dimension filter should stay on the metric input before the offset join so
  it constrains the source rows that are being shifted.
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
    time_spine_src_28006.ds AS metric_time__day
    , subq_26.__bookings AS bookings
  FROM ***************************.mf_time_spine time_spine_src_28006
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
        subq_18.metric_time__day AS metric_time__day
        , listings_latest_src_28000.country AS listing__country_latest
        , subq_18.__bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS __bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_18
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_18.listing = listings_latest_src_28000.listing_id
    ) subq_23
    WHERE listing__country_latest == 'us'
    GROUP BY
      metric_time__day
  ) subq_26
  ON
    time_spine_src_28006.ds - INTERVAL 5 day = subq_26.metric_time__day
) subq_32
