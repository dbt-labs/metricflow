test_name: test_conversion_metric_with_custom_granularity_filter
test_filename: test_custom_granularity.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , user_id AS user
    , 1 AS visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  metric_time__martian_day AS metric_time__martian_day
  , CAST(buys AS FLOAT64) / CAST(NULLIF(visits, 0) AS FLOAT64) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_23.metric_time__martian_day, subq_34.metric_time__martian_day) AS metric_time__martian_day
    , MAX(subq_23.visits) AS visits
    , MAX(subq_34.buys) AS buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['visits', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      metric_time__martian_day
      , SUM(visits) AS visits
    FROM (
      -- Read From CTE For node_id=sma_28019
      -- Join to Custom Granularity Dataset
      SELECT
        sma_28019_cte.visits AS visits
        , subq_19.martian_day AS metric_time__martian_day
      FROM sma_28019_cte sma_28019_cte
      LEFT OUTER JOIN
        ***************************.mf_time_spine subq_19
      ON
        sma_28019_cte.metric_time__day = subq_19.ds
    ) subq_20
    WHERE metric_time__martian_day = '2020-01-01'
    GROUP BY
      metric_time__martian_day
  ) subq_23
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['buys', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      metric_time__martian_day
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_27.visits) OVER (
          PARTITION BY
            subq_30.user
            , subq_30.metric_time__day
            , subq_30.mf_internal_uuid
          ORDER BY subq_27.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_27.metric_time__martian_day) OVER (
          PARTITION BY
            subq_30.user
            , subq_30.metric_time__day
            , subq_30.mf_internal_uuid
          ORDER BY subq_27.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__martian_day
        , FIRST_VALUE(subq_27.metric_time__day) OVER (
          PARTITION BY
            subq_30.user
            , subq_30.metric_time__day
            , subq_30.mf_internal_uuid
          ORDER BY subq_27.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_27.user) OVER (
          PARTITION BY
            subq_30.user
            , subq_30.metric_time__day
            , subq_30.mf_internal_uuid
          ORDER BY subq_27.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_30.mf_internal_uuid AS mf_internal_uuid
        , subq_30.buys AS buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['visits', 'metric_time__day', 'metric_time__martian_day', 'user']
        SELECT
          metric_time__martian_day
          , metric_time__day
          , subq_25.user
          , visits
        FROM (
          -- Read From CTE For node_id=sma_28019
          -- Join to Custom Granularity Dataset
          SELECT
            sma_28019_cte.metric_time__day AS metric_time__day
            , sma_28019_cte.user AS user
            , sma_28019_cte.visits AS visits
            , subq_24.martian_day AS metric_time__martian_day
          FROM sma_28019_cte sma_28019_cte
          LEFT OUTER JOIN
            ***************************.mf_time_spine subq_24
          ON
            sma_28019_cte.metric_time__day = subq_24.ds
        ) subq_25
        WHERE metric_time__martian_day = '2020-01-01'
      ) subq_27
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATETIME_TRUNC(ds, day) AS metric_time__day
          , user_id AS user
          , 1 AS buys
          , GENERATE_UUID() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_30
      ON
        (
          subq_27.user = subq_30.user
        ) AND (
          (
            subq_27.metric_time__day <= subq_30.metric_time__day
          ) AND (
            subq_27.metric_time__day > DATE_SUB(CAST(subq_30.metric_time__day AS DATETIME), INTERVAL 7 day)
          )
        )
    ) subq_31
    GROUP BY
      metric_time__martian_day
  ) subq_34
  ON
    subq_23.metric_time__martian_day = subq_34.metric_time__martian_day
  GROUP BY
    metric_time__martian_day
) subq_35