test_name: test_derived_fill_nulls_for_one_input_metric
test_filename: test_fill_nulls_with_rendering.py
sql_engine: BigQuery
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
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
      -- Aggregate Measures
      SELECT
        metric_time__day
        , SUM(bookings) AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['bookings', 'metric_time__day']
        SELECT
          DATETIME_TRUNC(ds, day) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_19
      GROUP BY
        metric_time__day
    ) subq_20
    ON
      subq_22.ds = subq_20.metric_time__day
  ) subq_23
)

, cm_7_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_28.ds AS metric_time__day
    , SUM(subq_26.bookings) AS bookings_2_weeks_ago
  FROM ***************************.mf_time_spine subq_28
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_26
  ON
    DATE_SUB(CAST(subq_28.ds AS DATETIME), INTERVAL 14 day) = subq_26.metric_time__day
  GROUP BY
    metric_time__day
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_24.metric_time__day, subq_32.metric_time__day) AS metric_time__day
      , COALESCE(MAX(subq_24.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
      , MAX(subq_32.bookings_2_weeks_ago) AS bookings_2_weeks_ago
    FROM (
      -- Read From CTE For node_id=cm_6
      SELECT
        metric_time__day
        , bookings_fill_nulls_with_0
      FROM cm_6_cte cm_6_cte
    ) subq_24
    FULL OUTER JOIN (
      -- Read From CTE For node_id=cm_7
      SELECT
        metric_time__day
        , bookings_2_weeks_ago
      FROM cm_7_cte cm_7_cte
    ) subq_32
    ON
      subq_24.metric_time__day = subq_32.metric_time__day
    GROUP BY
      metric_time__day
  ) subq_33
)

SELECT
  metric_time__day AS metric_time__day
  , bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset AS bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset
FROM cm_8_cte cm_8_cte
