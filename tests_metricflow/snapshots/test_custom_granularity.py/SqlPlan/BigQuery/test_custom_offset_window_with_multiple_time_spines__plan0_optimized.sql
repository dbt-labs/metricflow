test_name: test_custom_offset_window_with_multiple_time_spines
test_filename: test_custom_granularity.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  metric_time__hour
  , archived_users AS archived_users_offset_1_alien_day
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['archived_users', 'metric_time__hour']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_29.ts__hour__lead AS metric_time__hour
    , SUM(subq_24.archived_users) AS archived_users
  FROM (
    -- Offset Base Granularity By Custom Granularity Period(s)
    WITH cte_6 AS (
      -- Get Custom Granularity Bounds
      SELECT
        time_spine_src_28005.ts AS ts__hour
        , time_spine_src_28006.alien_day AS ds__alien_day
        , FIRST_VALUE(time_spine_src_28005.ts) OVER (
          PARTITION BY time_spine_src_28006.alien_day
          ORDER BY time_spine_src_28005.ts
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ts__hour__first_value
        , LAST_VALUE(time_spine_src_28005.ts) OVER (
          PARTITION BY time_spine_src_28006.alien_day
          ORDER BY time_spine_src_28005.ts
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ts__hour__last_value
        , ROW_NUMBER() OVER (
          PARTITION BY time_spine_src_28006.alien_day
          ORDER BY time_spine_src_28005.ts
        ) AS ts__hour__row_number
      FROM ***************************.mf_time_spine time_spine_src_28006
      INNER JOIN
        ***************************.mf_time_spine_hour time_spine_src_28005
      ON
        time_spine_src_28006.ds = DATETIME_TRUNC(time_spine_src_28005.ts, day)
    )

    SELECT
      cte_6.ts__hour AS ts__hour
      , CASE
        WHEN DATE_ADD(CAST(subq_28.ts__hour__first_value__lead AS DATETIME), INTERVAL cte_6.ts__hour__row_number - 1 hour) <= subq_28.ts__hour__last_value__lead
          THEN DATE_ADD(CAST(subq_28.ts__hour__first_value__lead AS DATETIME), INTERVAL cte_6.ts__hour__row_number - 1 hour)
        ELSE NULL
      END AS ts__hour__lead
    FROM cte_6 cte_6
    INNER JOIN (
      -- Offset Custom Granularity Bounds
      SELECT
        ds__alien_day
        , LEAD(ts__hour__first_value, 1) OVER (ORDER BY ds__alien_day) AS ts__hour__first_value__lead
        , LEAD(ts__hour__last_value, 1) OVER (ORDER BY ds__alien_day) AS ts__hour__last_value__lead
      FROM (
        -- Get Unique Rows for Custom Granularity Bounds
        SELECT
          ds__alien_day
          , ts__hour__first_value
          , ts__hour__last_value
        FROM cte_6 cte_6
        GROUP BY
          ds__alien_day
          , ts__hour__first_value
          , ts__hour__last_value
      ) subq_27
    ) subq_28
    ON
      cte_6.ds__alien_day = subq_28.ds__alien_day
  ) subq_29
  INNER JOIN (
    -- Read Elements From Semantic Model 'users_ds_source'
    -- Metric Time Dimension 'archived_at'
    SELECT
      DATETIME_TRUNC(archived_at, hour) AS metric_time__hour
      , 1 AS archived_users
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_24
  ON
    subq_29.ts__hour = subq_24.metric_time__hour
  GROUP BY
    metric_time__hour
) subq_35
