test_name: test_conversion_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , user_id AS user
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  metric_time__alien_day AS metric_time__alien_day
  , CAST(__buys AS FLOAT64) / CAST(NULLIF(__visits, 0) AS FLOAT64) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_21.metric_time__alien_day, subq_31.metric_time__alien_day) AS metric_time__alien_day
    , MAX(subq_21.__visits) AS __visits
    , MAX(subq_31.__buys) AS __buys
  FROM (
    -- Read From CTE For node_id=sma_28019
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['__visits', 'metric_time__alien_day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_18.alien_day AS metric_time__alien_day
      , SUM(sma_28019_cte.__visits) AS __visits
    FROM sma_28019_cte
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_18
    ON
      sma_28019_cte.metric_time__day = subq_18.ds
    GROUP BY
      metric_time__alien_day
  ) subq_21
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['__buys', 'metric_time__alien_day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__alien_day
      , SUM(__buys) AS __buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_24.__visits) OVER (
          PARTITION BY
            subq_27.user
            , subq_27.metric_time__day
            , subq_27.mf_internal_uuid
          ORDER BY subq_24.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS __visits
        , FIRST_VALUE(subq_24.metric_time__alien_day) OVER (
          PARTITION BY
            subq_27.user
            , subq_27.metric_time__day
            , subq_27.mf_internal_uuid
          ORDER BY subq_24.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__alien_day
        , FIRST_VALUE(subq_24.metric_time__day) OVER (
          PARTITION BY
            subq_27.user
            , subq_27.metric_time__day
            , subq_27.mf_internal_uuid
          ORDER BY subq_24.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_24.user) OVER (
          PARTITION BY
            subq_27.user
            , subq_27.metric_time__day
            , subq_27.mf_internal_uuid
          ORDER BY subq_24.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_27.mf_internal_uuid AS mf_internal_uuid
        , subq_27.__buys AS __buys
      FROM (
        -- Read From CTE For node_id=sma_28019
        -- Join to Custom Granularity Dataset
        -- Pass Only Elements: ['__visits', 'metric_time__day', 'metric_time__alien_day', 'user']
        SELECT
          subq_22.alien_day AS metric_time__alien_day
          , sma_28019_cte.metric_time__day AS metric_time__day
          , sma_28019_cte.user AS user
          , sma_28019_cte.__visits AS __visits
        FROM sma_28019_cte
        LEFT OUTER JOIN
          ***************************.mf_time_spine subq_22
        ON
          sma_28019_cte.metric_time__day = subq_22.ds
      ) subq_24
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATETIME_TRUNC(ds, day) AS metric_time__day
          , user_id AS user
          , 1 AS __buys
          , GENERATE_UUID() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_27
      ON
        (
          subq_24.user = subq_27.user
        ) AND (
          (
            subq_24.metric_time__day <= subq_27.metric_time__day
          ) AND (
            subq_24.metric_time__day > DATE_SUB(CAST(subq_27.metric_time__day AS DATETIME), INTERVAL 7 day)
          )
        )
    ) subq_28
    GROUP BY
      metric_time__alien_day
  ) subq_31
  ON
    subq_21.metric_time__alien_day = subq_31.metric_time__alien_day
  GROUP BY
    metric_time__alien_day
) subq_32
