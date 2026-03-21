test_name: test_nested_offsets_with_custom_grain
test_filename: test_custom_granularity.py
docstring:
  Check that a query for a nested offset metric does not select `metric_time` at different grains if not requested.

      It should not have `metric_time__day` in the output query.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH rss_28018_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS ds__day
    , alien_day AS ds__alien_day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , metric_time__alien_day AS metric_time__alien_day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_35.metric_time__alien_day AS metric_time__alien_day
    , subq_31.metric_time__day AS metric_time__day
    , subq_31.bookings_offset_once AS bookings_offset_once
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
  ) subq_35
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , metric_time__alien_day
      , 2 * bookings AS bookings_offset_once
    FROM (
      -- Join to Time Spine Dataset
      -- Compute Metrics via Expressions
      SELECT
        rss_28018_cte.ds__day AS metric_time__day
        , rss_28018_cte.ds__alien_day AS metric_time__alien_day
        , subq_24.__bookings AS bookings
      FROM rss_28018_cte
      INNER JOIN (
        -- Metric Time Dimension 'ds'
        -- Join to Custom Granularity Dataset
        -- Select: ['__bookings', 'metric_time__alien_day', 'metric_time__day']
        -- Select: ['__bookings', 'metric_time__alien_day', 'metric_time__day']
        -- Aggregate Inputs for Simple Metrics
        SELECT
          subq_20.alien_day AS metric_time__alien_day
          , subq_19.ds__day AS metric_time__day
          , SUM(subq_19.__bookings) AS __bookings
        FROM (
          -- Read Elements From Semantic Model 'bookings_source'
          SELECT
            1 AS __bookings
            , DATE_TRUNC('day', ds) AS ds__day
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_19
        LEFT OUTER JOIN
          ***************************.mf_time_spine subq_20
        ON
          subq_19.ds__day = subq_20.ds
        GROUP BY
          subq_20.alien_day
          , subq_19.ds__day
      ) subq_24
      ON
        rss_28018_cte.ds__day - INTERVAL 5 day = subq_24.metric_time__day
    ) subq_30
  ) subq_31
  ON
    subq_35.metric_time__alien_day - INTERVAL 2 day = subq_31.metric_time__alien_day
) subq_36
