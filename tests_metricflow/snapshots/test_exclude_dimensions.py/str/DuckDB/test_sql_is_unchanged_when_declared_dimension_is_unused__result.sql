test_name: test_sql_is_unchanged_when_declared_dimension_is_unused
test_filename: test_exclude_dimensions.py
docstring:
  The plan of a query that doesn't use the excluded dimension is the one of the metric without it.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , CAST(bookings AS DOUBLE) / CAST(NULLIF(views, 0) AS DOUBLE) AS bookings_per_view
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_5.metric_time__day, subq_11.metric_time__day) AS metric_time__day
    , MAX(subq_5.bookings) AS bookings
    , MAX(subq_11.views) AS views
  FROM (
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(__bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Select: ['__bookings', 'metric_time__day']
      -- Select: ['__bookings', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_10000
    ) subq_3
    GROUP BY
      metric_time__day
  ) subq_5
  FULL OUTER JOIN (
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(__views) AS views
    FROM (
      -- Read Elements From Semantic Model 'views_source'
      -- Metric Time Dimension 'ds'
      -- Select: ['__views', 'metric_time__day']
      -- Select: ['__views', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS __views
      FROM ***************************.fct_views views_source_src_10000
    ) subq_9
    GROUP BY
      metric_time__day
  ) subq_11
  ON
    subq_5.metric_time__day = subq_11.metric_time__day
  GROUP BY
    COALESCE(subq_5.metric_time__day, subq_11.metric_time__day)
) subq_12
