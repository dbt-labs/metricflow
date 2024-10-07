-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_23.metric_time__day, subq_34.metric_time__day) AS metric_time__day
    , COALESCE(MAX(subq_23.visits), 0) AS visits
    , COALESCE(MAX(subq_34.buys), 0) AS buys
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_22.ds AS metric_time__day
      , subq_20.visits AS visits
    FROM ***************************.mf_time_spine subq_22
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
      ) subq_19
      GROUP BY
        metric_time__day
    ) subq_20
    ON
      subq_22.ds = subq_20.metric_time__day
  ) subq_23
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    SELECT
      subq_33.ds AS metric_time__day
      , subq_31.buys AS buys
    FROM ***************************.mf_time_spine subq_33
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
          FIRST_VALUE(subq_25.visits) OVER (
            PARTITION BY
              subq_28.user
              , subq_28.ds__day
              , subq_28.mf_internal_uuid
            ORDER BY subq_25.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visits
          , FIRST_VALUE(subq_25.ds__day) OVER (
            PARTITION BY
              subq_28.user
              , subq_28.ds__day
              , subq_28.mf_internal_uuid
            ORDER BY subq_25.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS ds__day
          , FIRST_VALUE(subq_25.metric_time__day) OVER (
            PARTITION BY
              subq_28.user
              , subq_28.ds__day
              , subq_28.mf_internal_uuid
            ORDER BY subq_25.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__day
          , FIRST_VALUE(subq_25.user) OVER (
            PARTITION BY
              subq_28.user
              , subq_28.ds__day
              , subq_28.mf_internal_uuid
            ORDER BY subq_25.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_28.mf_internal_uuid AS mf_internal_uuid
          , subq_28.buys AS buys
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['visits', 'ds__day', 'metric_time__day', 'user']
          SELECT
            DATE_TRUNC('day', ds) AS ds__day
            , DATE_TRUNC('day', ds) AS metric_time__day
            , user_id AS user
            , 1 AS visits
          FROM ***************************.fct_visits visits_source_src_28000
        ) subq_25
        INNER JOIN (
          -- Read Elements From Semantic Model 'buys_source'
          -- Metric Time Dimension 'ds'
          -- Add column with generated UUID
          SELECT
            DATE_TRUNC('day', ds) AS ds__day
            , user_id AS user
            , 1 AS buys
            , GEN_RANDOM_UUID() AS mf_internal_uuid
          FROM ***************************.fct_buys buys_source_src_28000
        ) subq_28
        ON
          (
            subq_25.user = subq_28.user
          ) AND (
            (
              subq_25.ds__day <= subq_28.ds__day
            ) AND (
              subq_25.ds__day > subq_28.ds__day - INTERVAL 7 day
            )
          )
      ) subq_29
      GROUP BY
        metric_time__day
    ) subq_31
    ON
      subq_33.ds = subq_31.metric_time__day
  ) subq_34
  ON
    subq_23.metric_time__day = subq_34.metric_time__day
  GROUP BY
    COALESCE(subq_23.metric_time__day, subq_34.metric_time__day)
) subq_35
