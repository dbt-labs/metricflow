test_name: test_derived_fill_nulls_for_one_input_metric
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS __bookings
    , 1 AS __bookings_fill_nulls_with_0
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
    COALESCE(subq_33.metric_time__day, subq_43.metric_time__day) AS metric_time__day
    , COALESCE(MAX(subq_33.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , MAX(subq_43.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        rss_28018_cte.ds__day AS metric_time__day
        , subq_27.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
      FROM rss_28018_cte
      LEFT OUTER JOIN (
        -- Read From CTE For node_id=sma_28009
        -- Pass Only Elements: ['__bookings_fill_nulls_with_0', 'metric_time__day']
        -- Pass Only Elements: ['__bookings_fill_nulls_with_0', 'metric_time__day']
        -- Aggregate Inputs for Simple Metrics
        SELECT
          metric_time__day
          , SUM(__bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
        FROM sma_28009_cte
        GROUP BY
          metric_time__day
      ) subq_27
      ON
        rss_28018_cte.ds__day = subq_27.metric_time__day
    ) subq_32
  ) subq_33
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Compute Metrics via Expressions
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_37.__bookings AS bookings_2_weeks_ago
    FROM rss_28018_cte
    INNER JOIN (
      -- Read From CTE For node_id=sma_28009
      -- Pass Only Elements: ['__bookings', 'metric_time__day']
      -- Pass Only Elements: ['__bookings', 'metric_time__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        metric_time__day
        , SUM(__bookings) AS __bookings
      FROM sma_28009_cte
      GROUP BY
        metric_time__day
    ) subq_37
    ON
      rss_28018_cte.ds__day - MAKE_INTERVAL(days => 14) = subq_37.metric_time__day
  ) subq_43
  ON
    subq_33.metric_time__day = subq_43.metric_time__day
  GROUP BY
    COALESCE(subq_33.metric_time__day, subq_43.metric_time__day)
) subq_44
