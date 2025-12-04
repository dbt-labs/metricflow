test_name: test_conversion_metric_with_categorical_filter
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric with a categorical filter.
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
    , referrer_id AS visit__referrer_id
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , visit__referrer_id AS visit__referrer_id
  , CAST(__buys AS DOUBLE) / CAST(NULLIF(__visits, 0) AS DOUBLE) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_25.metric_time__day, subq_37.metric_time__day) AS metric_time__day
    , COALESCE(subq_25.visit__referrer_id, subq_37.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_25.__visits) AS __visits
    , MAX(subq_37.__buys) AS __buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , visit__referrer_id
      , SUM(visits) AS __visits
    FROM (
      -- Read From CTE For node_id=sma_28019
      -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'metric_time__day']
      SELECT
        metric_time__day
        , visit__referrer_id
        , __visits AS visits
      FROM sma_28019_cte
    ) subq_22
    WHERE visit__referrer_id = 'ref_id_01'
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) subq_25
  FULL OUTER JOIN (
    -- Find conversions for user within the range of INF
    -- Pass Only Elements: ['__buys', 'visit__referrer_id', 'metric_time__day']
    -- Pass Only Elements: ['__buys', 'visit__referrer_id', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , visit__referrer_id
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
        , FIRST_VALUE(subq_29.visit__referrer_id) OVER (
          PARTITION BY
            subq_32.user
            , subq_32.metric_time__day
            , subq_32.mf_internal_uuid
          ORDER BY subq_29.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
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
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'metric_time__day', 'user']
        SELECT
          metric_time__day
          , subq_27.user
          , visit__referrer_id
          , visits AS __visits
        FROM (
          -- Read From CTE For node_id=sma_28019
          -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'metric_time__day', 'user']
          SELECT
            metric_time__day
            , sma_28019_cte.user
            , visit__referrer_id
            , __visits AS visits
          FROM sma_28019_cte
        ) subq_27
        WHERE visit__referrer_id = 'ref_id_01'
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
          (subq_29.metric_time__day <= subq_32.metric_time__day)
        )
    ) subq_33
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) subq_37
  ON
    (
      subq_25.visit__referrer_id = subq_37.visit__referrer_id
    ) AND (
      subq_25.metric_time__day = subq_37.metric_time__day
    )
  GROUP BY
    COALESCE(subq_25.metric_time__day, subq_37.metric_time__day)
    , COALESCE(subq_25.visit__referrer_id, subq_37.visit__referrer_id)
) subq_38
