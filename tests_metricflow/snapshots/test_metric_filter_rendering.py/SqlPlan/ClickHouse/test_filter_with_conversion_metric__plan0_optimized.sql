test_name: test_filter_with_conversion_metric
test_filename: test_metric_filter_rendering.py
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
  SUM(__listings) AS listings
FROM (
  SELECT
    listings AS __listings
  FROM (
    SELECT
      CAST(subq_75.__buys AS Nullable(Float64)) / CAST(NULLIF(subq_75.__visits, 0) AS Nullable(Float64)) AS user__visit_buy_conversion_rate
      , subq_58.__listings AS listings
    FROM (
      SELECT
        user_id AS user
        , 1 AS __listings
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_58
    LEFT OUTER JOIN (
      SELECT
        COALESCE(subq_63.user, subq_74.user) AS user
        , MAX(subq_63.__visits) AS __visits
        , MAX(subq_74.__buys) AS __buys
      FROM (
        SELECT
          sma_28019_cte.user
          , SUM(__visits) AS __visits
        FROM sma_28019_cte
        GROUP BY
          sma_28019_cte.user
      ) subq_63
      FULL OUTER JOIN (
        SELECT
          subq_70.user
          , SUM(__buys) AS __buys
        FROM (
          SELECT DISTINCT
            FIRST_VALUE(sma_28019_cte.__visits) OVER (
              PARTITION BY
                subq_69.user
                , subq_69.metric_time__day
                , subq_69.mf_internal_uuid
              ORDER BY sma_28019_cte.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS __visits
            , FIRST_VALUE(sma_28019_cte.metric_time__day) OVER (
              PARTITION BY
                subq_69.user
                , subq_69.metric_time__day
                , subq_69.mf_internal_uuid
              ORDER BY sma_28019_cte.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS metric_time__day
            , FIRST_VALUE(sma_28019_cte.user) OVER (
              PARTITION BY
                subq_69.user
                , subq_69.metric_time__day
                , subq_69.mf_internal_uuid
              ORDER BY sma_28019_cte.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS user
            , subq_69.mf_internal_uuid AS mf_internal_uuid
            , subq_69.__buys AS __buys
          FROM sma_28019_cte
          INNER JOIN (
            SELECT
              toStartOfDay(ds) AS metric_time__day
              , user_id AS user
              , 1 AS __buys
              , generateUUIDv4() AS mf_internal_uuid
            FROM ***************************.fct_buys buys_source_src_28000
          ) subq_69
          ON
            (
              sma_28019_cte.user = subq_69.user
            ) AND (
              (sma_28019_cte.metric_time__day <= subq_69.metric_time__day)
            )
        ) subq_70
        GROUP BY
          subq_70.user
      ) subq_74
      ON
        subq_63.user = subq_74.user
      GROUP BY
        COALESCE(subq_63.user, subq_74.user)
    ) subq_75
    ON
      subq_58.user = subq_75.user
  ) subq_79
  WHERE user__visit_buy_conversion_rate > 2
) subq_81
