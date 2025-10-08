test_name: test_derived_fill_nulls_for_one_input_metric
test_filename: test_fill_nulls_with_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS bookings
    , 1 AS bookings_fill_nulls_with_0
  FROM ***************************.fct_bookings bookings_source_src_28000
)

, rss_28018_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS ds__day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_27.metric_time__day, subq_35.metric_time__day) AS metric_time__day
    , COALESCE(MAX(subq_27.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , MAX(subq_35.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , COALESCE(bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        rss_28018_cte.ds__day AS metric_time__day
        , subq_22.bookings_fill_nulls_with_0 AS bookings_fill_nulls_with_0
      FROM rss_28018_cte
      LEFT OUTER JOIN (
        -- Read From CTE For node_id=sma_28009
        -- Pass Only Elements: ['bookings_fill_nulls_with_0', 'metric_time__day']
        -- Aggregate Inputs for Simple Metrics
        SELECT
          metric_time__day
          , SUM(bookings_fill_nulls_with_0) AS bookings_fill_nulls_with_0
        FROM sma_28009_cte
        GROUP BY
          metric_time__day
      ) subq_22
      ON
        rss_28018_cte.ds__day = subq_22.metric_time__day
    ) subq_26
  ) subq_27
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Compute Metrics via Expressions
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_30.bookings_2_weeks_ago AS bookings_2_weeks_ago
    FROM rss_28018_cte
    INNER JOIN (
      -- Read From CTE For node_id=sma_28009
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        metric_time__day
        , SUM(bookings) AS bookings_2_weeks_ago
      FROM sma_28009_cte
      GROUP BY
        metric_time__day
    ) subq_30
    ON
      rss_28018_cte.ds__day - INTERVAL 14 day = subq_30.metric_time__day
  ) subq_35
  ON
    subq_27.metric_time__day = subq_35.metric_time__day
  GROUP BY
    COALESCE(subq_27.metric_time__day, subq_35.metric_time__day)
) subq_36
