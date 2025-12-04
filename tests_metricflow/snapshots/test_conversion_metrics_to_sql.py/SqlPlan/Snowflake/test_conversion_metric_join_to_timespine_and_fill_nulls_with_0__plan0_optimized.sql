test_name: test_conversion_metric_join_to_timespine_and_fill_nulls_with_0
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric that joins to time spine and fills nulls with 0.
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
    , 1 AS __visits_fill_nulls_with_0_join_to_timespine
  FROM ***************************.fct_visits visits_source_src_28000
)

, rss_28018_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS ds__day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , CAST(__buys_fill_nulls_with_0_join_to_timespine AS DOUBLE) / CAST(NULLIF(__visits_fill_nulls_with_0_join_to_timespine, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_37.metric_time__day, subq_53.metric_time__day) AS metric_time__day
    , COALESCE(MAX(subq_37.__visits_fill_nulls_with_0_join_to_timespine), 0) AS __visits_fill_nulls_with_0_join_to_timespine
    , COALESCE(MAX(subq_53.__buys_fill_nulls_with_0_join_to_timespine), 0) AS __buys_fill_nulls_with_0_join_to_timespine
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_32.__visits_fill_nulls_with_0_join_to_timespine AS __visits_fill_nulls_with_0_join_to_timespine
    FROM rss_28018_cte
    LEFT OUTER JOIN (
      -- Read From CTE For node_id=sma_28019
      -- Pass Only Elements: ['__visits_fill_nulls_with_0_join_to_timespine', 'metric_time__day']
      -- Pass Only Elements: ['__visits_fill_nulls_with_0_join_to_timespine', 'metric_time__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        metric_time__day
        , SUM(__visits_fill_nulls_with_0_join_to_timespine) AS __visits_fill_nulls_with_0_join_to_timespine
      FROM sma_28019_cte
      GROUP BY
        metric_time__day
    ) subq_32
    ON
      rss_28018_cte.ds__day = subq_32.metric_time__day
  ) subq_37
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_48.__buys_fill_nulls_with_0_join_to_timespine AS __buys_fill_nulls_with_0_join_to_timespine
    FROM rss_28018_cte
    LEFT OUTER JOIN (
      -- Find conversions for user within the range of 7 day
      -- Pass Only Elements: ['__buys_fill_nulls_with_0_join_to_timespine', 'metric_time__day']
      -- Pass Only Elements: ['__buys_fill_nulls_with_0_join_to_timespine', 'metric_time__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        metric_time__day
        , SUM(__buys_fill_nulls_with_0_join_to_timespine) AS __buys_fill_nulls_with_0_join_to_timespine
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          FIRST_VALUE(sma_28019_cte.__visits_fill_nulls_with_0_join_to_timespine) OVER (
            PARTITION BY
              subq_43.user
              , subq_43.metric_time__day
              , subq_43.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS __visits_fill_nulls_with_0_join_to_timespine
          , FIRST_VALUE(sma_28019_cte.metric_time__day) OVER (
            PARTITION BY
              subq_43.user
              , subq_43.metric_time__day
              , subq_43.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__day
          , FIRST_VALUE(sma_28019_cte.user) OVER (
            PARTITION BY
              subq_43.user
              , subq_43.metric_time__day
              , subq_43.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_43.mf_internal_uuid AS mf_internal_uuid
          , subq_43.__buys_fill_nulls_with_0_join_to_timespine AS __buys_fill_nulls_with_0_join_to_timespine
        FROM sma_28019_cte
        INNER JOIN (
          -- Read Elements From Semantic Model 'buys_source'
          -- Metric Time Dimension 'ds'
          -- Add column with generated UUID
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , user_id AS user
            , 1 AS __buys_fill_nulls_with_0_join_to_timespine
            , UUID_STRING() AS mf_internal_uuid
          FROM ***************************.fct_buys buys_source_src_28000
        ) subq_43
        ON
          (
            sma_28019_cte.user = subq_43.user
          ) AND (
            (
              sma_28019_cte.metric_time__day <= subq_43.metric_time__day
            ) AND (
              sma_28019_cte.metric_time__day > DATEADD(day, -7, subq_43.metric_time__day)
            )
          )
      ) subq_44
      GROUP BY
        metric_time__day
    ) subq_48
    ON
      rss_28018_cte.ds__day = subq_48.metric_time__day
  ) subq_53
  ON
    subq_37.metric_time__day = subq_53.metric_time__day
  GROUP BY
    COALESCE(subq_37.metric_time__day, subq_53.metric_time__day)
) subq_54
