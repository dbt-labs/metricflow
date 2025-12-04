test_name: test_conversion_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , user_id AS user
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  metric_time__alien_day AS metric_time__alien_day
  , CAST(__buys AS DOUBLE) / CAST(NULLIF(__visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_25.metric_time__alien_day, subq_37.metric_time__alien_day) AS metric_time__alien_day
    , MAX(subq_25.__visits) AS __visits
    , MAX(subq_37.__buys) AS __buys
  FROM (
    -- Read From CTE For node_id=sma_28019
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['__visits', 'metric_time__alien_day']
    -- Pass Only Elements: ['__visits', 'metric_time__alien_day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_21.alien_day AS metric_time__alien_day
      , SUM(sma_28019_cte.__visits) AS __visits
    FROM sma_28019_cte
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_21
    ON
      sma_28019_cte.metric_time__day = subq_21.ds
    GROUP BY
      subq_21.alien_day
  ) subq_25
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['__buys', 'metric_time__alien_day']
    -- Pass Only Elements: ['__buys', 'metric_time__alien_day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__alien_day
      , SUM(__buys) AS __buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_29.__visits) OVER (
          PARTITION BY
            subq_32.user
            , subq_32.metric_time__day
            , subq_32.mf_internal_uuid
          ORDER BY subq_29.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS __visits
        , FIRST_VALUE(subq_29.metric_time__alien_day) OVER (
          PARTITION BY
            subq_32.user
            , subq_32.metric_time__day
            , subq_32.mf_internal_uuid
          ORDER BY subq_29.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__alien_day
        , FIRST_VALUE(subq_29.metric_time__day) OVER (
          PARTITION BY
            subq_32.user
            , subq_32.metric_time__day
            , subq_32.mf_internal_uuid
          ORDER BY subq_29.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_29.user) OVER (
          PARTITION BY
            subq_32.user
            , subq_32.metric_time__day
            , subq_32.mf_internal_uuid
          ORDER BY subq_29.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_32.mf_internal_uuid AS mf_internal_uuid
        , subq_32.__buys AS __buys
      FROM (
        -- Read From CTE For node_id=sma_28019
        -- Join to Custom Granularity Dataset
        -- Pass Only Elements: ['__visits', 'metric_time__day', 'metric_time__alien_day', 'user']
        -- Pass Only Elements: ['__visits', 'metric_time__day', 'metric_time__alien_day', 'user']
        SELECT
          subq_26.alien_day AS metric_time__alien_day
          , sma_28019_cte.metric_time__day AS metric_time__day
          , sma_28019_cte.user AS user
          , sma_28019_cte.__visits AS __visits
        FROM sma_28019_cte
        LEFT OUTER JOIN
          ***************************.mf_time_spine subq_26
        ON
          sma_28019_cte.metric_time__day = subq_26.ds
      ) subq_29
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS __buys
          , UUID_STRING() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_32
      ON
        (
          subq_29.user = subq_32.user
        ) AND (
          (
            subq_29.metric_time__day <= subq_32.metric_time__day
          ) AND (
            subq_29.metric_time__day > DATEADD(day, -7, subq_32.metric_time__day)
          )
        )
    ) subq_33
    GROUP BY
      metric_time__alien_day
  ) subq_37
  ON
    subq_25.metric_time__alien_day = subq_37.metric_time__alien_day
  GROUP BY
    COALESCE(subq_25.metric_time__alien_day, subq_37.metric_time__alien_day)
) subq_38
