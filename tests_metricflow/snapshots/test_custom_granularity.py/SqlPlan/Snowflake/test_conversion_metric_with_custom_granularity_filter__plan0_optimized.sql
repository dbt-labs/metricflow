test_name: test_conversion_metric_with_custom_granularity_filter
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
    , 1 AS visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  metric_time__alien_day AS metric_time__alien_day
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_24.metric_time__alien_day, subq_35.metric_time__alien_day) AS metric_time__alien_day
    , MAX(subq_24.visits) AS visits
    , MAX(subq_35.buys) AS buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['visits', 'metric_time__alien_day']
    -- Aggregate Measures
    SELECT
      metric_time__alien_day
      , SUM(visits) AS visits
    FROM (
      -- Read From CTE For node_id=sma_28019
      -- Join to Custom Granularity Dataset
      SELECT
        sma_28019_cte.visits AS visits
        , subq_20.alien_day AS metric_time__alien_day
      FROM sma_28019_cte
      LEFT OUTER JOIN
        ***************************.mf_time_spine subq_20
      ON
        sma_28019_cte.metric_time__day = subq_20.ds
    ) subq_21
    WHERE metric_time__alien_day = '2020-01-01'
    GROUP BY
      metric_time__alien_day
  ) subq_24
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['buys', 'metric_time__alien_day']
    -- Aggregate Measures
    SELECT
      metric_time__alien_day
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_28.visits) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.metric_time__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_28.metric_time__alien_day) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.metric_time__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__alien_day
        , FIRST_VALUE(subq_28.metric_time__day) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.metric_time__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_28.user) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.metric_time__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_31.mf_internal_uuid AS mf_internal_uuid
        , subq_31.buys AS buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['visits', 'metric_time__day', 'metric_time__alien_day', 'user']
        SELECT
          metric_time__alien_day
          , metric_time__day
          , subq_26.user
          , visits
        FROM (
          -- Read From CTE For node_id=sma_28019
          -- Join to Custom Granularity Dataset
          SELECT
            sma_28019_cte.metric_time__day AS metric_time__day
            , sma_28019_cte.user AS user
            , sma_28019_cte.visits AS visits
            , subq_25.alien_day AS metric_time__alien_day
          FROM sma_28019_cte
          LEFT OUTER JOIN
            ***************************.mf_time_spine subq_25
          ON
            sma_28019_cte.metric_time__day = subq_25.ds
        ) subq_26
        WHERE metric_time__alien_day = '2020-01-01'
      ) subq_28
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS buys
          , UUID_STRING() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_31
      ON
        (
          subq_28.user = subq_31.user
        ) AND (
          (
            subq_28.metric_time__day <= subq_31.metric_time__day
          ) AND (
            subq_28.metric_time__day > DATEADD(day, -7, subq_31.metric_time__day)
          )
        )
    ) subq_32
    GROUP BY
      metric_time__alien_day
  ) subq_35
  ON
    subq_24.metric_time__alien_day = subq_35.metric_time__alien_day
  GROUP BY
    COALESCE(subq_24.metric_time__alien_day, subq_35.metric_time__alien_day)
) subq_36
