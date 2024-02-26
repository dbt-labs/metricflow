-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , CAST(buys AS DOUBLE PRECISION) / CAST(NULLIF(visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate_7days_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_27.metric_time__day, subq_40.metric_time__day) AS metric_time__day
    , COALESCE(MAX(subq_27.visits), 0) AS visits
    , COALESCE(MAX(subq_40.buys), 0) AS buys
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_26.ds AS metric_time__day
      , subq_24.visits AS visits
    FROM ***************************.mf_time_spine subq_26
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
      ) subq_23
      GROUP BY
        metric_time__day
    ) subq_24
    ON
      subq_26.ds = subq_24.metric_time__day
  ) subq_27
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    SELECT
      subq_39.ds AS metric_time__day
      , subq_37.buys AS buys
    FROM ***************************.mf_time_spine subq_39
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
          first_value(subq_30.visits) OVER (PARTITION BY subq_33.user, subq_33.ds__day, subq_33.mf_internal_uuid ORDER BY subq_30.ds__day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS visits
          , first_value(subq_30.ds__day) OVER (PARTITION BY subq_33.user, subq_33.ds__day, subq_33.mf_internal_uuid ORDER BY subq_30.ds__day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ds__day
          , first_value(subq_30.metric_time__day) OVER (PARTITION BY subq_33.user, subq_33.ds__day, subq_33.mf_internal_uuid ORDER BY subq_30.ds__day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS metric_time__day
          , first_value(subq_30.user) OVER (PARTITION BY subq_33.user, subq_33.ds__day, subq_33.mf_internal_uuid ORDER BY subq_30.ds__day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user
          , subq_33.mf_internal_uuid AS mf_internal_uuid
          , subq_33.buys AS buys
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
        ) subq_30
        INNER JOIN (
          -- Read Elements From Semantic Model 'buys_source'
          -- Metric Time Dimension 'ds'
          -- Add column with generated UUID
          SELECT
            DATE_TRUNC('day', ds) AS ds__day
            , user_id AS user
            , 1 AS buys
            , CONCAT(CAST(RANDOM()*100000000 AS INT)::VARCHAR,CAST(RANDOM()*100000000 AS INT)::VARCHAR) AS mf_internal_uuid
          FROM ***************************.fct_buys buys_source_src_28000
        ) subq_33
        ON
          (
            subq_30.user = subq_33.user
          ) AND (
            (
              subq_30.ds__day <= subq_33.ds__day
            ) AND (
              subq_30.ds__day > DATEADD(day, -7, subq_33.ds__day)
            )
          )
      ) subq_34
      GROUP BY
        metric_time__day
    ) subq_37
    ON
      subq_39.ds = subq_37.metric_time__day
  ) subq_40
  ON
    subq_27.metric_time__day = subq_40.metric_time__day
  GROUP BY
    COALESCE(subq_27.metric_time__day, subq_40.metric_time__day)
) subq_41
