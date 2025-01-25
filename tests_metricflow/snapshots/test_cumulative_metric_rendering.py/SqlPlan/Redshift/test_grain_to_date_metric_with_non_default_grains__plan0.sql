test_name: test_grain_to_date_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative grain to date metric queried with non-default grains.

      Uses agg time dimension instead of metric_time. Excludes default grain.
sql_engine: Redshift
---
-- Re-aggregate Metric via Group By
SELECT
  nr_subq_7.revenue_instance__ds__quarter
  , nr_subq_7.revenue_instance__ds__year
  , nr_subq_7.revenue_mtd
FROM (
  -- Window Function for Metric Re-aggregation
  SELECT
    nr_subq_6.revenue_instance__ds__quarter
    , nr_subq_6.revenue_instance__ds__year
    , FIRST_VALUE(nr_subq_6.revenue_mtd) OVER (
      PARTITION BY
        nr_subq_6.revenue_instance__ds__quarter
        , nr_subq_6.revenue_instance__ds__year
      ORDER BY nr_subq_6.metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS revenue_mtd
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      nr_subq_5.revenue_instance__ds__quarter
      , nr_subq_5.revenue_instance__ds__year
      , nr_subq_5.metric_time__day
      , nr_subq_5.txn_revenue AS revenue_mtd
    FROM (
      -- Aggregate Measures
      SELECT
        nr_subq_4.revenue_instance__ds__quarter
        , nr_subq_4.revenue_instance__ds__year
        , nr_subq_4.metric_time__day
        , SUM(nr_subq_4.txn_revenue) AS txn_revenue
      FROM (
        -- Pass Only Elements: ['txn_revenue', 'revenue_instance__ds__quarter', 'revenue_instance__ds__year', 'metric_time__day']
        SELECT
          nr_subq_3.revenue_instance__ds__quarter
          , nr_subq_3.revenue_instance__ds__year
          , nr_subq_3.metric_time__day
          , nr_subq_3.txn_revenue
        FROM (
          -- Join Self Over Time Range
          SELECT
            nr_subq_1.revenue_instance__ds__quarter AS revenue_instance__ds__quarter
            , nr_subq_1.revenue_instance__ds__year AS revenue_instance__ds__year
            , nr_subq_1.metric_time__day AS metric_time__day
            , nr_subq_0.ds__day AS ds__day
            , nr_subq_0.ds__week AS ds__week
            , nr_subq_0.ds__month AS ds__month
            , nr_subq_0.ds__quarter AS ds__quarter
            , nr_subq_0.ds__year AS ds__year
            , nr_subq_0.ds__extract_year AS ds__extract_year
            , nr_subq_0.ds__extract_quarter AS ds__extract_quarter
            , nr_subq_0.ds__extract_month AS ds__extract_month
            , nr_subq_0.ds__extract_day AS ds__extract_day
            , nr_subq_0.ds__extract_dow AS ds__extract_dow
            , nr_subq_0.ds__extract_doy AS ds__extract_doy
            , nr_subq_0.revenue_instance__ds__day AS revenue_instance__ds__day
            , nr_subq_0.revenue_instance__ds__week AS revenue_instance__ds__week
            , nr_subq_0.revenue_instance__ds__month AS revenue_instance__ds__month
            , nr_subq_0.revenue_instance__ds__extract_year AS revenue_instance__ds__extract_year
            , nr_subq_0.revenue_instance__ds__extract_quarter AS revenue_instance__ds__extract_quarter
            , nr_subq_0.revenue_instance__ds__extract_month AS revenue_instance__ds__extract_month
            , nr_subq_0.revenue_instance__ds__extract_day AS revenue_instance__ds__extract_day
            , nr_subq_0.revenue_instance__ds__extract_dow AS revenue_instance__ds__extract_dow
            , nr_subq_0.revenue_instance__ds__extract_doy AS revenue_instance__ds__extract_doy
            , nr_subq_0.metric_time__week AS metric_time__week
            , nr_subq_0.metric_time__month AS metric_time__month
            , nr_subq_0.metric_time__quarter AS metric_time__quarter
            , nr_subq_0.metric_time__year AS metric_time__year
            , nr_subq_0.metric_time__extract_year AS metric_time__extract_year
            , nr_subq_0.metric_time__extract_quarter AS metric_time__extract_quarter
            , nr_subq_0.metric_time__extract_month AS metric_time__extract_month
            , nr_subq_0.metric_time__extract_day AS metric_time__extract_day
            , nr_subq_0.metric_time__extract_dow AS metric_time__extract_dow
            , nr_subq_0.metric_time__extract_doy AS metric_time__extract_doy
            , nr_subq_0.user AS user
            , nr_subq_0.revenue_instance__user AS revenue_instance__user
            , nr_subq_0.txn_revenue AS txn_revenue
          FROM (
            -- Read From Time Spine 'mf_time_spine'
            SELECT
              DATE_TRUNC('quarter', nr_subq_2.ds) AS revenue_instance__ds__quarter
              , DATE_TRUNC('year', nr_subq_2.ds) AS revenue_instance__ds__year
              , nr_subq_2.ds AS metric_time__day
            FROM ***************************.mf_time_spine nr_subq_2
          ) nr_subq_1
          INNER JOIN (
            -- Metric Time Dimension 'ds'
            SELECT
              nr_subq_28008.ds__day
              , nr_subq_28008.ds__week
              , nr_subq_28008.ds__month
              , nr_subq_28008.ds__quarter
              , nr_subq_28008.ds__year
              , nr_subq_28008.ds__extract_year
              , nr_subq_28008.ds__extract_quarter
              , nr_subq_28008.ds__extract_month
              , nr_subq_28008.ds__extract_day
              , nr_subq_28008.ds__extract_dow
              , nr_subq_28008.ds__extract_doy
              , nr_subq_28008.revenue_instance__ds__day
              , nr_subq_28008.revenue_instance__ds__week
              , nr_subq_28008.revenue_instance__ds__month
              , nr_subq_28008.revenue_instance__ds__quarter
              , nr_subq_28008.revenue_instance__ds__year
              , nr_subq_28008.revenue_instance__ds__extract_year
              , nr_subq_28008.revenue_instance__ds__extract_quarter
              , nr_subq_28008.revenue_instance__ds__extract_month
              , nr_subq_28008.revenue_instance__ds__extract_day
              , nr_subq_28008.revenue_instance__ds__extract_dow
              , nr_subq_28008.revenue_instance__ds__extract_doy
              , nr_subq_28008.ds__day AS metric_time__day
              , nr_subq_28008.ds__week AS metric_time__week
              , nr_subq_28008.ds__month AS metric_time__month
              , nr_subq_28008.ds__quarter AS metric_time__quarter
              , nr_subq_28008.ds__year AS metric_time__year
              , nr_subq_28008.ds__extract_year AS metric_time__extract_year
              , nr_subq_28008.ds__extract_quarter AS metric_time__extract_quarter
              , nr_subq_28008.ds__extract_month AS metric_time__extract_month
              , nr_subq_28008.ds__extract_day AS metric_time__extract_day
              , nr_subq_28008.ds__extract_dow AS metric_time__extract_dow
              , nr_subq_28008.ds__extract_doy AS metric_time__extract_doy
              , nr_subq_28008.user
              , nr_subq_28008.revenue_instance__user
              , nr_subq_28008.txn_revenue
            FROM (
              -- Read Elements From Semantic Model 'revenue'
              SELECT
                revenue_src_28000.revenue AS txn_revenue
                , DATE_TRUNC('day', revenue_src_28000.created_at) AS ds__day
                , DATE_TRUNC('week', revenue_src_28000.created_at) AS ds__week
                , DATE_TRUNC('month', revenue_src_28000.created_at) AS ds__month
                , DATE_TRUNC('quarter', revenue_src_28000.created_at) AS ds__quarter
                , DATE_TRUNC('year', revenue_src_28000.created_at) AS ds__year
                , EXTRACT(year FROM revenue_src_28000.created_at) AS ds__extract_year
                , EXTRACT(quarter FROM revenue_src_28000.created_at) AS ds__extract_quarter
                , EXTRACT(month FROM revenue_src_28000.created_at) AS ds__extract_month
                , EXTRACT(day FROM revenue_src_28000.created_at) AS ds__extract_day
                , CASE WHEN EXTRACT(dow FROM revenue_src_28000.created_at) = 0 THEN EXTRACT(dow FROM revenue_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM revenue_src_28000.created_at) END AS ds__extract_dow
                , EXTRACT(doy FROM revenue_src_28000.created_at) AS ds__extract_doy
                , DATE_TRUNC('day', revenue_src_28000.created_at) AS revenue_instance__ds__day
                , DATE_TRUNC('week', revenue_src_28000.created_at) AS revenue_instance__ds__week
                , DATE_TRUNC('month', revenue_src_28000.created_at) AS revenue_instance__ds__month
                , DATE_TRUNC('quarter', revenue_src_28000.created_at) AS revenue_instance__ds__quarter
                , DATE_TRUNC('year', revenue_src_28000.created_at) AS revenue_instance__ds__year
                , EXTRACT(year FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_year
                , EXTRACT(quarter FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_quarter
                , EXTRACT(month FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_month
                , EXTRACT(day FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_day
                , CASE WHEN EXTRACT(dow FROM revenue_src_28000.created_at) = 0 THEN EXTRACT(dow FROM revenue_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM revenue_src_28000.created_at) END AS revenue_instance__ds__extract_dow
                , EXTRACT(doy FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
                , revenue_src_28000.user_id AS user
                , revenue_src_28000.user_id AS revenue_instance__user
              FROM ***************************.fct_revenue revenue_src_28000
            ) nr_subq_28008
          ) nr_subq_0
          ON
            (
              nr_subq_0.metric_time__day <= nr_subq_1.metric_time__day
            ) AND (
              nr_subq_0.metric_time__day >= DATE_TRUNC('month', nr_subq_1.metric_time__day)
            )
        ) nr_subq_3
      ) nr_subq_4
      GROUP BY
        nr_subq_4.revenue_instance__ds__quarter
        , nr_subq_4.revenue_instance__ds__year
        , nr_subq_4.metric_time__day
    ) nr_subq_5
  ) nr_subq_6
) nr_subq_7
GROUP BY
  nr_subq_7.revenue_instance__ds__quarter
  , nr_subq_7.revenue_instance__ds__year
  , nr_subq_7.revenue_mtd
