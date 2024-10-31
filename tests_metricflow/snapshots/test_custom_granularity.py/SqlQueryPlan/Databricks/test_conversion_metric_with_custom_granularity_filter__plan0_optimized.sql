-- Compute Metrics via Expressions
SELECT
  metric_time__martian_day
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_27.metric_time__martian_day, subq_40.metric_time__martian_day) AS metric_time__martian_day
    , MAX(subq_27.visits) AS visits
    , MAX(subq_40.buys) AS buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['visits', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      metric_time__martian_day
      , SUM(visits) AS visits
    FROM (
      -- Pass Only Elements: ['visits', 'metric_time__day']
      -- Join to Custom Granularity Dataset
      SELECT
        subq_22.visits AS visits
        , subq_23.martian_day AS metric_time__martian_day
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) subq_22
      LEFT OUTER JOIN
        ***************************.mf_time_spine subq_23
      ON
        subq_22.metric_time__day = subq_23.ds
    ) subq_24
    WHERE metric_time__martian_day = '2020-01-01'
    GROUP BY
      metric_time__martian_day
  ) subq_27
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['buys', 'metric_time__martian_day']
    -- Pass Only Elements: ['buys', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      metric_time__martian_day
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_32.visits) OVER (
          PARTITION BY
            subq_35.user
            , subq_35.metric_time__day
            , subq_35.mf_internal_uuid
          ORDER BY subq_32.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_32.metric_time__martian_day) OVER (
          PARTITION BY
            subq_35.user
            , subq_35.metric_time__day
            , subq_35.mf_internal_uuid
          ORDER BY subq_32.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__martian_day
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
        , subq_35.buys AS buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['visits', 'metric_time__day', 'metric_time__martian_day', 'user']
        SELECT
          metric_time__martian_day
          , metric_time__day
          , subq_30.user
          , visits
        FROM (
          -- Metric Time Dimension 'ds'
          -- Join to Custom Granularity Dataset
          SELECT
            subq_28.ds__day AS metric_time__day
            , subq_28.user AS user
            , subq_28.visits AS visits
            , subq_29.martian_day AS metric_time__martian_day
          FROM (
            -- Read Elements From Semantic Model 'visits_source'
            SELECT
              1 AS visits
              , DATE_TRUNC('day', ds) AS ds__day
              , user_id AS user
            FROM ***************************.fct_visits visits_source_src_28000
          ) subq_28
          LEFT OUTER JOIN
            ***************************.mf_time_spine subq_29
          ON
            subq_28.ds__day = subq_29.ds
        ) subq_30
        WHERE metric_time__martian_day = '2020-01-01'
      ) subq_32
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS buys
          , UUID() AS mf_internal_uuid
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
      metric_time__martian_day
  ) subq_40
  ON
    subq_27.metric_time__martian_day = subq_40.metric_time__martian_day
  GROUP BY
    COALESCE(subq_27.metric_time__martian_day, subq_40.metric_time__martian_day)
) subq_41
