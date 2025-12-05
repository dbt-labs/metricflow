test_name: test_conversion_metric_with_custom_granularity_filter
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , user_id AS user
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  metric_time__alien_day AS metric_time__alien_day
  , CAST(__buys AS DOUBLE) / CAST(NULLIF(__visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_28.metric_time__alien_day, subq_41.metric_time__alien_day) AS metric_time__alien_day
    , MAX(subq_28.__visits) AS __visits
    , MAX(subq_41.__buys) AS __buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__visits', 'metric_time__alien_day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__alien_day
      , SUM(visits) AS __visits
    FROM (
      -- Read From CTE For node_id=sma_28019
      -- Join to Custom Granularity Dataset
      -- Pass Only Elements: ['__visits', 'metric_time__alien_day']
      SELECT
        subq_23.alien_day AS metric_time__alien_day
        , sma_28019_cte.__visits AS visits
      FROM sma_28019_cte
      LEFT OUTER JOIN
        ***************************.mf_time_spine subq_23
      ON
        sma_28019_cte.metric_time__day = subq_23.ds
    ) subq_25
    WHERE metric_time__alien_day = '2020-01-01'
    GROUP BY
      metric_time__alien_day
  ) subq_28
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['__buys', 'metric_time__alien_day']
    -- Pass Only Elements: ['__buys', 'metric_time__alien_day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__alien_day
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
        , FIRST_VALUE(subq_33.metric_time__alien_day) OVER (
          PARTITION BY
            subq_36.user
            , subq_36.metric_time__day
            , subq_36.mf_internal_uuid
          ORDER BY subq_33.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__alien_day
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
        -- Pass Only Elements: ['__visits', 'metric_time__day', 'metric_time__alien_day', 'user']
        SELECT
          metric_time__alien_day
          , metric_time__day
          , subq_31.user
          , visits AS __visits
        FROM (
          -- Read From CTE For node_id=sma_28019
          -- Join to Custom Granularity Dataset
          -- Pass Only Elements: ['__visits', 'metric_time__day', 'metric_time__alien_day', 'user']
          SELECT
            subq_29.alien_day AS metric_time__alien_day
            , sma_28019_cte.metric_time__day AS metric_time__day
            , sma_28019_cte.user AS user
            , sma_28019_cte.__visits AS visits
          FROM sma_28019_cte
          LEFT OUTER JOIN
            ***************************.mf_time_spine subq_29
          ON
            sma_28019_cte.metric_time__day = subq_29.ds
        ) subq_31
        WHERE metric_time__alien_day = '2020-01-01'
      ) subq_33
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS __buys
          , uuid() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_36
      ON
        (
          subq_33.user = subq_36.user
        ) AND (
          (
            subq_33.metric_time__day <= subq_36.metric_time__day
          ) AND (
            subq_33.metric_time__day > DATE_ADD('day', -7, subq_36.metric_time__day)
          )
        )
    ) subq_37
    GROUP BY
      metric_time__alien_day
  ) subq_41
  ON
    subq_28.metric_time__alien_day = subq_41.metric_time__alien_day
  GROUP BY
    COALESCE(subq_28.metric_time__alien_day, subq_41.metric_time__alien_day)
) subq_42
