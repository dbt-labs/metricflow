test_name: test_nested_offset_metric_and_simple_metric_with_custom_grain
test_filename: test_custom_granularity.py
docstring:
  Check that metric with a nested offset metric can be queried with the associated simple metric.
sql_engine: DuckDB
---
-- Combine Aggregated Outputs
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

, rss_28018_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS ds__day
    , alien_day AS ds__alien_day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  COALESCE(subq_46.metric_time__alien_day, subq_52.metric_time__alien_day) AS metric_time__alien_day
  , MAX(subq_46.bookings_offset_twice) AS bookings_offset_twice
  , MAX(subq_52.bookings) AS bookings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__alien_day
    , 2 * bookings_offset_once AS bookings_offset_twice
  FROM (
    -- Join to Time Spine Dataset
    -- Select: ['metric_time__alien_day', 'bookings_offset_once']
    SELECT
      subq_43.metric_time__alien_day AS metric_time__alien_day
      , subq_39.bookings_offset_once AS bookings_offset_once
    FROM (
      -- Read From CTE For node_id=rss_28018
      -- Change Column Aliases
      -- Select: ['metric_time__alien_day']
      -- Select: ['metric_time__alien_day']
      SELECT
        ds__alien_day AS metric_time__alien_day
      FROM rss_28018_cte
      GROUP BY
        ds__alien_day
    ) subq_43
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__alien_day
        , 2 * bookings AS bookings_offset_once
      FROM (
        -- Join to Time Spine Dataset
        -- Compute Metrics via Expressions
        SELECT
          rss_28018_cte.ds__day AS metric_time__day
          , rss_28018_cte.ds__alien_day AS metric_time__alien_day
          , subq_32.__bookings AS bookings
        FROM rss_28018_cte
        INNER JOIN (
          -- Read From CTE For node_id=sma_28009
          -- Join to Custom Granularity Dataset
          -- Select: ['__bookings', 'metric_time__alien_day', 'metric_time__day']
          -- Select: ['__bookings', 'metric_time__alien_day', 'metric_time__day']
          -- Aggregate Inputs for Simple Metrics
          SELECT
            subq_28.alien_day AS metric_time__alien_day
            , sma_28009_cte.metric_time__day AS metric_time__day
            , SUM(sma_28009_cte.__bookings) AS __bookings
          FROM sma_28009_cte
          LEFT OUTER JOIN
            ***************************.mf_time_spine subq_28
          ON
            sma_28009_cte.metric_time__day = subq_28.ds
          GROUP BY
            subq_28.alien_day
            , sma_28009_cte.metric_time__day
        ) subq_32
        ON
          rss_28018_cte.ds__day - INTERVAL 5 day = subq_32.metric_time__day
      ) subq_38
    ) subq_39
    ON
      subq_43.metric_time__alien_day - INTERVAL 2 day = subq_39.metric_time__alien_day
  ) subq_45
) subq_46
FULL OUTER JOIN (
  -- Read From CTE For node_id=sma_28009
  -- Join to Custom Granularity Dataset
  -- Select: ['__bookings', 'metric_time__alien_day']
  -- Select: ['__bookings', 'metric_time__alien_day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    subq_47.alien_day AS metric_time__alien_day
    , SUM(sma_28009_cte.__bookings) AS bookings
  FROM sma_28009_cte
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_47
  ON
    sma_28009_cte.metric_time__day = subq_47.ds
  GROUP BY
    subq_47.alien_day
) subq_52
ON
  subq_46.metric_time__alien_day = subq_52.metric_time__alien_day
GROUP BY
  COALESCE(subq_46.metric_time__alien_day, subq_52.metric_time__alien_day)
