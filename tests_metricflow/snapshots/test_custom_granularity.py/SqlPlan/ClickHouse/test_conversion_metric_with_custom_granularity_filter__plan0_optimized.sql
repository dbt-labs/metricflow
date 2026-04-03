test_name: test_conversion_metric_with_custom_granularity_filter
test_filename: test_custom_granularity.py
sql_engine: ClickHouse
---
WITH sma_28019_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , user_id AS user
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  metric_time__alien_day AS metric_time__alien_day
  , CAST(__buys AS Nullable(Float64)) / CAST(NULLIF(__visits, 0) AS Nullable(Float64)) AS visit_buy_conversion_rate_7days
FROM (
  SELECT
    COALESCE(subq_28.metric_time__alien_day, subq_41.metric_time__alien_day) AS metric_time__alien_day
    , MAX(subq_28.__visits) AS __visits
    , MAX(subq_41.__buys) AS __buys
  FROM (
    SELECT
      metric_time__alien_day
      , SUM(__visits) AS __visits
    FROM (
      SELECT
        metric_time__alien_day
        , visits AS __visits
      FROM (
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
    ) subq_27
    GROUP BY
      metric_time__alien_day
  ) subq_28
  FULL OUTER JOIN (
    SELECT
      metric_time__alien_day
      , SUM(__buys) AS __buys
    FROM (
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
        SELECT
          metric_time__alien_day
          , metric_time__day
          , subq_31.user
          , visits AS __visits
        FROM (
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
        SELECT
          toStartOfDay(ds) AS metric_time__day
          , user_id AS user
          , 1 AS __buys
          , generateUUIDv4() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_36
      ON
        (
          subq_33.user = subq_36.user
        ) AND (
          (
            subq_33.metric_time__day <= subq_36.metric_time__day
          ) AND (
            subq_33.metric_time__day > addDays(subq_36.metric_time__day, -7)
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
