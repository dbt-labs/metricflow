test_name: test_conversion_metric_with_time_constraint
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric with a time constraint and categorical filter.
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH ctr_1_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , user_id AS user
    , referrer_id AS visit__referrer_id
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
  WHERE DATETIME_TRUNC(ds, day) BETWEEN '2020-01-01' AND '2020-01-02'
)

SELECT
  visit__referrer_id AS visit__referrer_id
  , CAST(__buys AS FLOAT64) / CAST(NULLIF(__visits, 0) AS FLOAT64) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_29.visit__referrer_id, subq_41.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_29.__visits) AS __visits
    , MAX(subq_41.__buys) AS __buys
  FROM (
    -- Constrain Output with WHERE
    -- Select: ['__visits', 'visit__referrer_id']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      visit__referrer_id
      , SUM(visits) AS __visits
    FROM (
      -- Read From CTE For node_id=ctr_1
      -- Select: ['__visits', 'visit__referrer_id']
      SELECT
        visit__referrer_id
        , __visits AS visits
      FROM ctr_1_cte
    ) subq_26
    WHERE visit__referrer_id = 'ref_id_01'
    GROUP BY
      visit__referrer_id
  ) subq_29
  FULL OUTER JOIN (
    -- Find conversions for user within the range of INF
    -- Select: ['__buys', 'visit__referrer_id']
    -- Select: ['__buys', 'visit__referrer_id']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      visit__referrer_id
      , SUM(__buys) AS __buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_33.__visits) OVER (
          PARTITION BY
            subq_36.user
            , subq_36.metric_time__day
            , subq_36.mf_internal_uuid
          ORDER BY subq_33.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS __visits
        , FIRST_VALUE(subq_33.visit__referrer_id) OVER (
          PARTITION BY
            subq_36.user
            , subq_36.metric_time__day
            , subq_36.mf_internal_uuid
          ORDER BY subq_33.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(subq_33.metric_time__day) OVER (
          PARTITION BY
            subq_36.user
            , subq_36.metric_time__day
            , subq_36.mf_internal_uuid
          ORDER BY subq_33.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_33.user) OVER (
          PARTITION BY
            subq_36.user
            , subq_36.metric_time__day
            , subq_36.mf_internal_uuid
          ORDER BY subq_33.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_36.mf_internal_uuid AS mf_internal_uuid
        , subq_36.__buys AS __buys
      FROM (
        -- Constrain Output with WHERE
        -- Select: ['__visits', 'visit__referrer_id', 'metric_time__day', 'user']
        SELECT
          metric_time__day
          , subq_31.user
          , visit__referrer_id
          , visits AS __visits
        FROM (
          -- Read From CTE For node_id=ctr_1
          -- Select: ['__visits', 'visit__referrer_id', 'metric_time__day', 'user']
          SELECT
            metric_time__day
            , ctr_1_cte.user
            , visit__referrer_id
            , __visits AS visits
          FROM ctr_1_cte
        ) subq_31
        WHERE visit__referrer_id = 'ref_id_01'
      ) subq_33
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
      ) subq_36
      ON
        (
          subq_33.user = subq_36.user
        ) AND (
          (subq_33.metric_time__day <= subq_36.metric_time__day)
        )
    ) subq_37
    GROUP BY
      visit__referrer_id
  ) subq_41
  ON
    subq_29.visit__referrer_id = subq_41.visit__referrer_id
  GROUP BY
    visit__referrer_id
) subq_42
