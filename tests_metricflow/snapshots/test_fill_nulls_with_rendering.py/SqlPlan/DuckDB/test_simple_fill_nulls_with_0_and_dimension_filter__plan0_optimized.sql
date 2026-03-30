test_name: test_simple_fill_nulls_with_0_and_dimension_filter
test_filename: test_fill_nulls_with_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_25.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
  FROM ***************************.mf_time_spine time_spine_src_28006
  LEFT OUTER JOIN (
    -- Constrain Output with WHERE
    -- Select: ['__bookings_fill_nulls_with_0', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , SUM(bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
    FROM (
      -- Join Standard Outputs
      -- Select: ['__bookings_fill_nulls_with_0', 'listing__capacity_latest', 'metric_time__day']
      SELECT
        subq_17.metric_time__day AS metric_time__day
        , listings_latest_src_28000.capacity AS listing__capacity_latest
        , subq_17.__bookings_fill_nulls_with_0 AS bookings_fill_nulls_with_0
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , 1 AS __bookings_fill_nulls_with_0
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_17
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_17.listing = listings_latest_src_28000.listing_id
    ) subq_22
    WHERE listing__capacity_latest > 3
    GROUP BY
      metric_time__day
  ) subq_25
  ON
    time_spine_src_28006.ds = subq_25.metric_time__day
) subq_30
