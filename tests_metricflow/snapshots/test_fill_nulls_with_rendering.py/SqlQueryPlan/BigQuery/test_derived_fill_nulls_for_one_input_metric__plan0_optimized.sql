test_name: test_derived_fill_nulls_for_one_input_metric
test_filename: test_fill_nulls_with_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_24.metric_time__day, subq_31.metric_time__day) AS metric_time__day
    , COALESCE(MAX(subq_24.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , MAX(subq_31.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_22.ds AS metric_time__day
        , subq_20.bookings AS bookings
      FROM ***************************.mf_time_spine subq_22
      LEFT OUTER JOIN (
        -- Read From CTE For node_id=sma_28009
        -- Pass Only Elements: ['bookings', 'metric_time__day']
        -- Aggregate Measures
        SELECT
          metric_time__day
          , SUM(bookings) AS bookings
        FROM sma_28009_cte sma_28009_cte
        GROUP BY
          metric_time__day
      ) subq_20
      ON
        subq_22.ds = subq_20.metric_time__day
    ) subq_23
  ) subq_24
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_27.ds AS metric_time__day
      , SUM(sma_28009_cte.bookings) AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine subq_27
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      DATE_SUB(CAST(subq_27.ds AS DATETIME), INTERVAL 14 day) = sma_28009_cte.metric_time__day
    GROUP BY
      metric_time__day
  ) subq_31
  ON
    subq_24.metric_time__day = subq_31.metric_time__day
  GROUP BY
    metric_time__day
) subq_32
