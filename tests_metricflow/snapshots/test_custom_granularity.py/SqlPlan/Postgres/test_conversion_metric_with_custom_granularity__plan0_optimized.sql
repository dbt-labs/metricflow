test_name: test_conversion_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
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
  metric_time__martian_day AS metric_time__martian_day
  , CAST(buys AS DOUBLE PRECISION) / CAST(NULLIF(visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_21.metric_time__martian_day, subq_31.metric_time__martian_day) AS metric_time__martian_day
    , MAX(subq_21.visits) AS visits
    , MAX(subq_31.buys) AS buys
  FROM (
    -- Read From CTE For node_id=sma_28019
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['visits', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      subq_18.martian_day AS metric_time__martian_day
      , SUM(sma_28019_cte.visits) AS visits
    FROM sma_28019_cte sma_28019_cte
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_18
    ON
      sma_28019_cte.metric_time__day = subq_18.ds
    GROUP BY
      subq_18.martian_day
  ) subq_21
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
        FIRST_VALUE(subq_24.visits) OVER (
          PARTITION BY
            subq_27.user
            , subq_27.metric_time__day
            , subq_27.mf_internal_uuid
          ORDER BY subq_24.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_24.metric_time__martian_day) OVER (
          PARTITION BY
            subq_27.user
            , subq_27.metric_time__day
            , subq_27.mf_internal_uuid
          ORDER BY subq_24.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__martian_day
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
        , subq_27.buys AS buys
      FROM (
        -- Read From CTE For node_id=sma_28019
        -- Join to Custom Granularity Dataset
        -- Pass Only Elements: ['visits', 'metric_time__day', 'metric_time__martian_day', 'user']
        SELECT
          subq_22.martian_day AS metric_time__martian_day
          , sma_28019_cte.metric_time__day AS metric_time__day
          , sma_28019_cte.user AS user
          , sma_28019_cte.visits AS visits
        FROM sma_28019_cte sma_28019_cte
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
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS buys
          , GEN_RANDOM_UUID() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_27
      ON
        (
          subq_24.user = subq_27.user
        ) AND (
          (
            subq_24.metric_time__day <= subq_27.metric_time__day
          ) AND (
            subq_24.metric_time__day > subq_27.metric_time__day - MAKE_INTERVAL(days => 7)
          )
        )
    ) subq_28
    GROUP BY
      metric_time__martian_day
  ) subq_31
  ON
    subq_21.metric_time__martian_day = subq_31.metric_time__martian_day
  GROUP BY
    COALESCE(subq_21.metric_time__martian_day, subq_31.metric_time__martian_day)
) subq_32