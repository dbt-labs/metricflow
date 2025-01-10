test_name: test_conversion_metric_with_custom_granularity_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: BigQuery
---
-- Combine Aggregated Outputs
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
  CAST(MAX(subq_35.buys) AS FLOAT64) / CAST(NULLIF(MAX(subq_24.visits), 0) AS FLOAT64) AS visit_buy_conversion_rate_7days
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['visits',]
  -- Aggregate Measures
  SELECT
    SUM(visits) AS visits
  FROM (
    -- Read From CTE For node_id=sma_28019
    -- Join to Custom Granularity Dataset
    SELECT
      sma_28019_cte.visits AS visits
      , subq_20.martian_day AS metric_time__martian_day
    FROM sma_28019_cte sma_28019_cte
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_20
    ON
      sma_28019_cte.metric_time__day = subq_20.ds
  ) subq_21
  WHERE metric_time__martian_day = '2020-01-01'
) subq_24
CROSS JOIN (
  -- Find conversions for user within the range of 7 day
  -- Pass Only Elements: ['buys',]
  -- Aggregate Measures
  SELECT
    SUM(buys) AS buys
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
      , FIRST_VALUE(subq_28.metric_time__martian_day) OVER (
        PARTITION BY
          subq_31.user
          , subq_31.metric_time__day
          , subq_31.mf_internal_uuid
        ORDER BY subq_28.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__martian_day
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
      -- Pass Only Elements: ['visits', 'metric_time__day', 'metric_time__martian_day', 'user']
      SELECT
        metric_time__martian_day
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
          , subq_25.martian_day AS metric_time__martian_day
        FROM sma_28019_cte sma_28019_cte
        LEFT OUTER JOIN
          ***************************.mf_time_spine subq_25
        ON
          sma_28019_cte.metric_time__day = subq_25.ds
      ) subq_26
      WHERE metric_time__martian_day = '2020-01-01'
    ) subq_28
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
    ) subq_31
    ON
      (
        subq_28.user = subq_31.user
      ) AND (
        (
          subq_28.metric_time__day <= subq_31.metric_time__day
        ) AND (
          subq_28.metric_time__day > DATE_SUB(CAST(subq_31.metric_time__day AS DATETIME), INTERVAL 7 day)
        )
      )
  ) subq_32
) subq_35