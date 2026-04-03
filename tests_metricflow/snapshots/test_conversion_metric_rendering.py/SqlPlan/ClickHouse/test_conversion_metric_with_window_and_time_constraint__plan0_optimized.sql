test_name: test_conversion_metric_with_window_and_time_constraint
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric with a window, time constraint, and categorical filter.
sql_engine: ClickHouse
---
WITH ctr_1_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , user_id AS user
    , referrer_id AS visit__referrer_id
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
  WHERE toStartOfDay(ds) BETWEEN '2020-01-01' AND '2020-01-02'
)

SELECT
  metric_time__day AS metric_time__day
  , visit__referrer_id AS visit__referrer_id
  , CAST(__buys AS Nullable(Float64)) / CAST(NULLIF(__visits, 0) AS Nullable(Float64)) AS visit_buy_conversion_rate_7days
FROM (
  SELECT
    COALESCE(subq_29.metric_time__day, subq_41.metric_time__day) AS metric_time__day
    , COALESCE(subq_29.visit__referrer_id, subq_41.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_29.__visits) AS __visits
    , MAX(subq_41.__buys) AS __buys
  FROM (
    SELECT
      metric_time__day
      , visit__referrer_id
      , SUM(__visits) AS __visits
    FROM (
      SELECT
        metric_time__day
        , visit__referrer_id
        , visits AS __visits
      FROM (
        SELECT
          metric_time__day
          , visit__referrer_id
          , __visits AS visits
        FROM ctr_1_cte
      ) subq_26
      WHERE visit__referrer_id = 'ref_id_01'
    ) subq_28
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) subq_29
  FULL OUTER JOIN (
    SELECT
      metric_time__day
      , visit__referrer_id
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
        , FIRST_VALUE(subq_33.visit__referrer_id) OVER (
          PARTITION BY
            subq_36.user
            , subq_36.metric_time__day
            , subq_36.mf_internal_uuid
          ORDER BY subq_33.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
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
          metric_time__day
          , subq_31.user
          , visit__referrer_id
          , visits AS __visits
        FROM (
          SELECT
            metric_time__day
            , ctr_1_cte.user
            , visit__referrer_id
            , __visits AS visits
          FROM ctr_1_cte
        ) subq_31
        WHERE visit__referrer_id = 'ref_id_01'
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
      metric_time__day
      , visit__referrer_id
  ) subq_41
  ON
    (
      subq_29.visit__referrer_id = subq_41.visit__referrer_id
    ) AND (
      subq_29.metric_time__day = subq_41.metric_time__day
    )
  GROUP BY
    COALESCE(subq_29.metric_time__day, subq_41.metric_time__day)
    , COALESCE(subq_29.visit__referrer_id, subq_41.visit__referrer_id)
) subq_42
