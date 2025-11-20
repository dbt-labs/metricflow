test_name: test_conversion_metric_with_time_constraint
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric with a time constraint and categorical filter.
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH ctr_0_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , user_id AS user
    , referrer_id AS visit__referrer_id
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
  WHERE DATE_TRUNC('day', ds) BETWEEN '2020-01-01' AND '2020-01-02'
)

SELECT
  visit__referrer_id AS visit__referrer_id
  , CAST(__buys AS DOUBLE) / CAST(NULLIF(__visits, 0) AS DOUBLE) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_24.visit__referrer_id, subq_34.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_24.__visits) AS __visits
    , MAX(subq_34.__buys) AS __buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__visits', 'visit__referrer_id']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      visit__referrer_id
      , SUM(visits) AS __visits
    FROM (
      -- Read From CTE For node_id=ctr_0
      SELECT
        visit__referrer_id
        , __visits AS visits
      FROM ctr_0_cte
    ) subq_21
    WHERE visit__referrer_id = 'ref_id_01'
    GROUP BY
      visit__referrer_id
  ) subq_24
  FULL OUTER JOIN (
    -- Find conversions for user within the range of INF
    -- Pass Only Elements: ['__buys', 'visit__referrer_id']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      visit__referrer_id
      , SUM(__buys) AS __buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_27.__visits) OVER (
          PARTITION BY
            subq_30.user
            , subq_30.metric_time__day
            , subq_30.mf_internal_uuid
          ORDER BY subq_27.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS __visits
        , FIRST_VALUE(subq_27.visit__referrer_id) OVER (
          PARTITION BY
            subq_30.user
            , subq_30.metric_time__day
            , subq_30.mf_internal_uuid
          ORDER BY subq_27.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
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
        , subq_30.__buys AS __buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'metric_time__day', 'user']
        SELECT
          metric_time__day
          , subq_25.user
          , visit__referrer_id
          , visits AS __visits
        FROM (
          -- Read From CTE For node_id=ctr_0
          SELECT
            metric_time__day
            , ctr_0_cte.user
            , visit__referrer_id
            , __visits AS visits
          FROM ctr_0_cte
        ) subq_25
        WHERE visit__referrer_id = 'ref_id_01'
      ) subq_27
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
      ) subq_30
      ON
        (
          subq_27.user = subq_30.user
        ) AND (
          (subq_27.metric_time__day <= subq_30.metric_time__day)
        )
    ) subq_31
    GROUP BY
      visit__referrer_id
  ) subq_34
  ON
    subq_24.visit__referrer_id = subq_34.visit__referrer_id
  GROUP BY
    COALESCE(subq_24.visit__referrer_id, subq_34.visit__referrer_id)
) subq_35
