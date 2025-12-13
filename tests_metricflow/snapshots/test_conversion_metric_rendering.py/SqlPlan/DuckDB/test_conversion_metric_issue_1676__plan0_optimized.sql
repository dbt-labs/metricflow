test_name: test_conversion_metric_issue_1676
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , DATE_TRUNC('month', ds) AS metric_time__month
    , user_id AS user
    , referrer_id AS visit__referrer_id
    , 1 AS visits
  FROM ***************************.fct_visits visits_source_src_28000
)

, rss_28018_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    DATE_TRUNC('month', ds) AS ds__month
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__month AS metric_time__month
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_issue_1676
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_33.metric_time__month, subq_47.metric_time__month) AS metric_time__month
    , COALESCE(MAX(subq_33.visits), 0) AS visits
    , COALESCE(MAX(subq_47.buys), 0) AS buys
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_32.metric_time__month AS metric_time__month
      , subq_29.visits AS visits
    FROM (
      -- Read From CTE For node_id=rss_28018
      -- Change Column Aliases
      -- Pass Only Elements: ['metric_time__month']
      SELECT
        ds__month AS metric_time__month
      FROM rss_28018_cte
      GROUP BY
        ds__month
    ) subq_32
    LEFT OUTER JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['visits', 'metric_time__month']
      -- Aggregate Measures
      SELECT
        metric_time__month
        , SUM(visits) AS visits
      FROM (
        -- Read From CTE For node_id=sma_28019
        SELECT
          metric_time__month
          , visit__referrer_id
          , visits
        FROM sma_28019_cte
      ) subq_26
      WHERE visit__referrer_id
      GROUP BY
        metric_time__month
    ) subq_29
    ON
      subq_32.metric_time__month = subq_29.metric_time__month
  ) subq_33
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    SELECT
      subq_46.metric_time__month AS metric_time__month
      , subq_43.buys AS buys
    FROM (
      -- Read From CTE For node_id=rss_28018
      -- Change Column Aliases
      -- Pass Only Elements: ['metric_time__month']
      SELECT
        ds__month AS metric_time__month
      FROM rss_28018_cte
      GROUP BY
        ds__month
    ) subq_46
    LEFT OUTER JOIN (
      -- Find conversions for user within the range of 7 day
      -- Pass Only Elements: ['buys', 'metric_time__month']
      -- Aggregate Measures
      SELECT
        metric_time__month
        , SUM(buys) AS buys
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          FIRST_VALUE(subq_36.visits) OVER (
            PARTITION BY
              subq_39.user
              , subq_39.metric_time__day
              , subq_39.mf_internal_uuid
            ORDER BY subq_36.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visits
          , FIRST_VALUE(subq_36.visit__referrer_id) OVER (
            PARTITION BY
              subq_39.user
              , subq_39.metric_time__day
              , subq_39.mf_internal_uuid
            ORDER BY subq_36.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visit__referrer_id
          , FIRST_VALUE(subq_36.metric_time__day) OVER (
            PARTITION BY
              subq_39.user
              , subq_39.metric_time__day
              , subq_39.mf_internal_uuid
            ORDER BY subq_36.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__day
          , FIRST_VALUE(subq_36.metric_time__month) OVER (
            PARTITION BY
              subq_39.user
              , subq_39.metric_time__day
              , subq_39.mf_internal_uuid
            ORDER BY subq_36.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__month
          , FIRST_VALUE(subq_36.user) OVER (
            PARTITION BY
              subq_39.user
              , subq_39.metric_time__day
              , subq_39.mf_internal_uuid
            ORDER BY subq_36.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_39.mf_internal_uuid AS mf_internal_uuid
          , subq_39.buys AS buys
        FROM (
          -- Constrain Output with WHERE
          -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day', 'metric_time__month', 'user']
          SELECT
            metric_time__day
            , metric_time__month
            , subq_34.user
            , visit__referrer_id
            , visits
          FROM (
            -- Read From CTE For node_id=sma_28019
            SELECT
              metric_time__day
              , metric_time__month
              , sma_28019_cte.user
              , visit__referrer_id
              , visits
            FROM sma_28019_cte
          ) subq_34
          WHERE visit__referrer_id
        ) subq_36
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
        ) subq_39
        ON
          (
            subq_36.user = subq_39.user
          ) AND (
            (
              subq_36.metric_time__day <= subq_39.metric_time__day
            ) AND (
              subq_36.metric_time__day > subq_39.metric_time__day - INTERVAL 7 day
            )
          )
      ) subq_40
      GROUP BY
        metric_time__month
    ) subq_43
    ON
      subq_46.metric_time__month = subq_43.metric_time__month
  ) subq_47
  ON
    subq_33.metric_time__month = subq_47.metric_time__month
  GROUP BY
    COALESCE(subq_33.metric_time__month, subq_47.metric_time__month)
) subq_48
