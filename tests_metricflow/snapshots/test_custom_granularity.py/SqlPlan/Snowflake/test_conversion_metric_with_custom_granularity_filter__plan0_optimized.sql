test_name: test_conversion_metric_with_custom_granularity_filter
test_filename: test_custom_granularity.py
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
SELECT
  metric_time__martian_day
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_20.metric_time__martian_day, nr_subq_30.metric_time__martian_day) AS metric_time__martian_day
    , MAX(nr_subq_20.visits) AS visits
    , MAX(nr_subq_30.buys) AS buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['visits', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      metric_time__martian_day
      , SUM(visits) AS visits
    FROM (
      -- Metric Time Dimension 'ds'
      -- Join to Custom Granularity Dataset
      SELECT
        nr_subq_28012.visits AS visits
        , nr_subq_16.martian_day AS metric_time__martian_day
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        SELECT
          1 AS visits
          , DATE_TRUNC('day', ds) AS ds__day
          , user_id AS user
        FROM ***************************.fct_visits visits_source_src_28000
      ) nr_subq_28012
      LEFT OUTER JOIN
        ***************************.mf_time_spine nr_subq_16
      ON
        nr_subq_28012.ds__day = nr_subq_16.ds
    ) nr_subq_17
    WHERE metric_time__martian_day = '2020-01-01'
    GROUP BY
      metric_time__martian_day
  ) nr_subq_20
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['buys', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      metric_time__martian_day
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(nr_subq_24.visits) OVER (
          PARTITION BY
            nr_subq_26.user
            , nr_subq_26.metric_time__day
            , nr_subq_26.mf_internal_uuid
          ORDER BY nr_subq_24.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(nr_subq_24.metric_time__martian_day) OVER (
          PARTITION BY
            nr_subq_26.user
            , nr_subq_26.metric_time__day
            , nr_subq_26.mf_internal_uuid
          ORDER BY nr_subq_24.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__martian_day
        , FIRST_VALUE(nr_subq_24.metric_time__day) OVER (
          PARTITION BY
            nr_subq_26.user
            , nr_subq_26.metric_time__day
            , nr_subq_26.mf_internal_uuid
          ORDER BY nr_subq_24.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(nr_subq_24.user) OVER (
          PARTITION BY
            nr_subq_26.user
            , nr_subq_26.metric_time__day
            , nr_subq_26.mf_internal_uuid
          ORDER BY nr_subq_24.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , nr_subq_26.mf_internal_uuid AS mf_internal_uuid
        , nr_subq_26.buys AS buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['visits', 'metric_time__day', 'metric_time__martian_day', 'user']
        SELECT
          metric_time__martian_day
          , metric_time__day
          , nr_subq_22.user
          , visits
        FROM (
          -- Metric Time Dimension 'ds'
          -- Join to Custom Granularity Dataset
          SELECT
            nr_subq_28012.ds__day AS metric_time__day
            , nr_subq_28012.user AS user
            , nr_subq_28012.visits AS visits
            , nr_subq_21.martian_day AS metric_time__martian_day
          FROM (
            -- Read Elements From Semantic Model 'visits_source'
            SELECT
              1 AS visits
              , DATE_TRUNC('day', ds) AS ds__day
              , user_id AS user
            FROM ***************************.fct_visits visits_source_src_28000
          ) nr_subq_28012
          LEFT OUTER JOIN
            ***************************.mf_time_spine nr_subq_21
          ON
            nr_subq_28012.ds__day = nr_subq_21.ds
        ) nr_subq_22
        WHERE metric_time__martian_day = '2020-01-01'
      ) nr_subq_24
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
      ) nr_subq_26
      ON
        (
          nr_subq_24.user = nr_subq_26.user
        ) AND (
          (
            nr_subq_24.metric_time__day <= nr_subq_26.metric_time__day
          ) AND (
            nr_subq_24.metric_time__day > DATEADD(day, -7, nr_subq_26.metric_time__day)
          )
        )
    ) nr_subq_27
    GROUP BY
      metric_time__martian_day
  ) nr_subq_30
  ON
    nr_subq_20.metric_time__martian_day = nr_subq_30.metric_time__martian_day
  GROUP BY
    COALESCE(nr_subq_20.metric_time__martian_day, nr_subq_30.metric_time__martian_day)
) nr_subq_31
