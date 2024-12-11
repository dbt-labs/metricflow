test_name: test_derived_metric_with_offset_window_and_offset_to_grain_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__year AS metric_time__year
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_24.metric_time__year, subq_31.metric_time__year) AS metric_time__year
    , MAX(subq_24.month_start_bookings) AS month_start_bookings
    , MAX(subq_31.bookings_1_month_ago) AS bookings_1_month_ago
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('year', subq_20.ds) AS metric_time__year
      , SUM(sma_28009_cte.bookings) AS month_start_bookings
    FROM ***************************.mf_time_spine subq_20
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      DATE_TRUNC('month', subq_20.ds) = sma_28009_cte.metric_time__day
    WHERE DATE_TRUNC('year', subq_20.ds) = subq_20.ds
    GROUP BY
      DATE_TRUNC('year', subq_20.ds)
  ) subq_24
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('year', subq_27.ds) AS metric_time__year
      , SUM(sma_28009_cte.bookings) AS bookings_1_month_ago
    FROM ***************************.mf_time_spine subq_27
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      subq_27.ds - INTERVAL 1 month = sma_28009_cte.metric_time__day
    GROUP BY
      DATE_TRUNC('year', subq_27.ds)
  ) subq_31
  ON
    subq_24.metric_time__year = subq_31.metric_time__year
  GROUP BY
    COALESCE(subq_24.metric_time__year, subq_31.metric_time__year)
) subq_32
