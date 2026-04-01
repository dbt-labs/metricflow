test_name: test_offset_metric_with_string_filter
test_filename: test_offset_metrics_with_filters.py
sql_engine: DuckDB
expectation_description:
  This test uses TRUE as a placeholder for arbitrary opaque SQL. Opaque SQL
  predicates are not analyzed for safe splitting and are assumed not to reference
  aggregation time dimensions. The current expectation is that the filter should
  be pushed to the pre-aggregation branch. However, the appropriate behavior may
  be to not push at all.
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
    subq_24.metric_time__day AS metric_time__day
    , subq_19.__bookings AS bookings
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
    ) subq_22
    WHERE TRUE
  ) subq_24
  INNER JOIN (
    -- Constrain Output with WHERE
    -- Select: ['__bookings', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , SUM(bookings) AS __bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Select: ['__bookings', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_16
    WHERE TRUE
    GROUP BY
      metric_time__day
  ) subq_19
  ON
    subq_24.metric_time__day - INTERVAL 5 day = subq_19.metric_time__day
) subq_26
