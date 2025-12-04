test_name: test_conversion_metric_with_window_and_time_constraint
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric with a window, time constraint, and categorical filter.
sql_engine: Redshift
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
  metric_time__day AS metric_time__day
  , visit__referrer_id AS visit__referrer_id
  , CAST(__buys AS DOUBLE PRECISION) / CAST(NULLIF(__visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_28.metric_time__day, subq_40.metric_time__day) AS metric_time__day
    , COALESCE(subq_28.visit__referrer_id, subq_40.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_28.__visits) AS __visits
    , MAX(subq_40.__buys) AS __buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , visit__referrer_id
      , SUM(visits) AS __visits
    FROM (
      -- Read From CTE For node_id=ctr_0
      -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'metric_time__day']
      SELECT
        metric_time__day
        , visit__referrer_id
        , __visits AS visits
      FROM ctr_0_cte
    ) subq_25
    WHERE visit__referrer_id = 'ref_id_01'
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) subq_28
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
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
        FIRST_VALUE(subq_32.__visits) OVER (
          PARTITION BY
            subq_35.user
            , subq_35.metric_time__day
            , subq_35.mf_internal_uuid
          ORDER BY subq_32.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS __visits
        , FIRST_VALUE(subq_32.visit__referrer_id) OVER (
          PARTITION BY
            subq_35.user
            , subq_35.metric_time__day
            , subq_35.mf_internal_uuid
          ORDER BY subq_32.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(subq_32.metric_time__day) OVER (
          PARTITION BY
            subq_35.user
            , subq_35.metric_time__day
            , subq_35.mf_internal_uuid
          ORDER BY subq_32.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_32.user) OVER (
          PARTITION BY
            subq_35.user
            , subq_35.metric_time__day
            , subq_35.mf_internal_uuid
          ORDER BY subq_32.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_35.mf_internal_uuid AS mf_internal_uuid
        , subq_35.__buys AS __buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'metric_time__day', 'user']
        SELECT
          metric_time__day
          , subq_30.user
          , visit__referrer_id
          , visits AS __visits
        FROM (
          -- Read From CTE For node_id=ctr_0
          -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'metric_time__day', 'user']
          SELECT
            metric_time__day
            , ctr_0_cte.user
            , visit__referrer_id
            , __visits AS visits
          FROM ctr_0_cte
        ) subq_30
        WHERE visit__referrer_id = 'ref_id_01'
      ) subq_32
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS __buys
          , CONCAT(CAST(RANDOM()*100000000 AS INT)::VARCHAR,CAST(RANDOM()*100000000 AS INT)::VARCHAR) AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_35
      ON
        (
          subq_32.user = subq_35.user
        ) AND (
          (
            subq_32.metric_time__day <= subq_35.metric_time__day
          ) AND (
            subq_32.metric_time__day > DATEADD(day, -7, subq_35.metric_time__day)
          )
        )
    ) subq_36
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) subq_40
  ON
    (
      subq_28.visit__referrer_id = subq_40.visit__referrer_id
    ) AND (
      subq_28.metric_time__day = subq_40.metric_time__day
    )
  GROUP BY
    COALESCE(subq_28.metric_time__day, subq_40.metric_time__day)
    , COALESCE(subq_28.visit__referrer_id, subq_40.visit__referrer_id)
) subq_41
