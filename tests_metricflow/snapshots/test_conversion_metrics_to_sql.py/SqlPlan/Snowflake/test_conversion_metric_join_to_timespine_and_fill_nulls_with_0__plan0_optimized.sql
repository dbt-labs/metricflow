test_name: test_conversion_metric_join_to_timespine_and_fill_nulls_with_0
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric that joins to time spine and fills nulls with 0.
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_6.metric_time__day, nr_subq_18.metric_time__day) AS metric_time__day
    , COALESCE(MAX(nr_subq_6.visits), 0) AS visits
    , COALESCE(MAX(nr_subq_18.buys), 0) AS buys
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , nr_subq_2.visits AS visits
    FROM ***************************.mf_time_spine time_spine_src_28006
    LEFT OUTER JOIN (
      -- Aggregate Measures
      SELECT
        metric_time__day
        , SUM(visits) AS visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['visits', 'metric_time__day']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) nr_subq_1
      GROUP BY
        metric_time__day
    ) nr_subq_2
    ON
      time_spine_src_28006.ds = nr_subq_2.metric_time__day
  ) nr_subq_6
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , nr_subq_14.buys AS buys
    FROM ***************************.mf_time_spine time_spine_src_28006
    LEFT OUTER JOIN (
      -- Find conversions for user within the range of 7 day
      -- Pass Only Elements: ['buys', 'metric_time__day']
      -- Aggregate Measures
      SELECT
        metric_time__day
        , SUM(buys) AS buys
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          FIRST_VALUE(nr_subq_8.visits) OVER (
            PARTITION BY
              nr_subq_10.user
              , nr_subq_10.metric_time__day
              , nr_subq_10.mf_internal_uuid
            ORDER BY nr_subq_8.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visits
          , FIRST_VALUE(nr_subq_8.metric_time__day) OVER (
            PARTITION BY
              nr_subq_10.user
              , nr_subq_10.metric_time__day
              , nr_subq_10.mf_internal_uuid
            ORDER BY nr_subq_8.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__day
          , FIRST_VALUE(nr_subq_8.user) OVER (
            PARTITION BY
              nr_subq_10.user
              , nr_subq_10.metric_time__day
              , nr_subq_10.mf_internal_uuid
            ORDER BY nr_subq_8.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , nr_subq_10.mf_internal_uuid AS mf_internal_uuid
          , nr_subq_10.buys AS buys
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , user_id AS user
            , 1 AS visits
          FROM ***************************.fct_visits visits_source_src_28000
        ) nr_subq_8
        INNER JOIN (
          -- Read Elements From Semantic Model 'buys_source'
          -- Metric Time Dimension 'ds'
          -- Add column with generated UUID
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , user_id AS user
            , 1 AS buys
            , UUID_STRING() AS mf_internal_uuid
          FROM ***************************.fct_buys buys_source_src_28000
        ) nr_subq_10
        ON
          (
            nr_subq_8.user = nr_subq_10.user
          ) AND (
            (
              nr_subq_8.metric_time__day <= nr_subq_10.metric_time__day
            ) AND (
              nr_subq_8.metric_time__day > DATEADD(day, -7, nr_subq_10.metric_time__day)
            )
          )
      ) nr_subq_11
      GROUP BY
        metric_time__day
    ) nr_subq_14
    ON
      time_spine_src_28006.ds = nr_subq_14.metric_time__day
  ) nr_subq_18
  ON
    nr_subq_6.metric_time__day = nr_subq_18.metric_time__day
  GROUP BY
    COALESCE(nr_subq_6.metric_time__day, nr_subq_18.metric_time__day)
) nr_subq_19
