test_name: test_derived_fill_nulls_for_one_input_metric
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Redshift
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
      , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        rss_28018_cte.ds__day AS metric_time__day
        , subq_22.bookings AS bookings
      FROM rss_28018_cte rss_28018_cte
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
      ) subq_22
      ON
        rss_28018_cte.ds__day = subq_22.metric_time__day
    ) subq_26
  ) subq_27
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , SUM(sma_28009_cte.bookings) AS bookings_2_weeks_ago
    FROM rss_28018_cte rss_28018_cte
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      DATEADD(day, -14, rss_28018_cte.ds__day) = sma_28009_cte.metric_time__day
    GROUP BY
      rss_28018_cte.ds__day
  ) subq_35
  ON
    subq_27.metric_time__day = subq_35.metric_time__day
  GROUP BY
    COALESCE(subq_27.metric_time__day, subq_35.metric_time__day)
) subq_36
