-- Re-aggregate Metric via Group By
SELECT
  subq_8.metric_time__week
  , subq_8.revenue_all_time
FROM (
  -- Window Function for Metric Re-aggregation
  SELECT
    subq_7.metric_time__week
    , LAST_VALUE(subq_7.revenue_all_time) OVER (
      PARTITION BY subq_7.metric_time__week
      ORDER BY subq_7.metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS revenue_all_time
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_6.metric_time__day
      , subq_6.metric_time__week
      , subq_6.txn_revenue AS revenue_all_time
    FROM (
      -- Aggregate Measures
      SELECT
        subq_5.metric_time__day
        , subq_5.metric_time__week
        , SUM(subq_5.txn_revenue) AS txn_revenue
      FROM (
        -- Pass Only Elements: ['txn_revenue', 'metric_time__week', 'metric_time__day']
        SELECT
          subq_4.metric_time__day
          , subq_4.metric_time__week
          , subq_4.txn_revenue
        FROM (
          -- Join Self Over Time Range
          SELECT
            subq_2.metric_time__day AS metric_time__day
            , subq_2.metric_time__week AS metric_time__week
            , subq_1.ds__day AS ds__day
            , subq_1.ds__week AS ds__week
            , subq_1.ds__month AS ds__month
            , subq_1.ds__quarter AS ds__quarter
            , subq_1.ds__year AS ds__year
            , subq_1.ds__extract_year AS ds__extract_year
            , subq_1.ds__extract_quarter AS ds__extract_quarter
            , subq_1.ds__extract_month AS ds__extract_month
            , subq_1.ds__extract_day AS ds__extract_day
            , subq_1.ds__extract_dow AS ds__extract_dow
            , subq_1.ds__extract_doy AS ds__extract_doy
            , subq_1.revenue_instance__ds__day AS revenue_instance__ds__day
            , subq_1.revenue_instance__ds__week AS revenue_instance__ds__week
            , subq_1.revenue_instance__ds__month AS revenue_instance__ds__month
            , subq_1.revenue_instance__ds__quarter AS revenue_instance__ds__quarter
            , subq_1.revenue_instance__ds__year AS revenue_instance__ds__year
            , subq_1.revenue_instance__ds__extract_year AS revenue_instance__ds__extract_year
            , subq_1.revenue_instance__ds__extract_quarter AS revenue_instance__ds__extract_quarter
            , subq_1.revenue_instance__ds__extract_month AS revenue_instance__ds__extract_month
            , subq_1.revenue_instance__ds__extract_day AS revenue_instance__ds__extract_day
            , subq_1.revenue_instance__ds__extract_dow AS revenue_instance__ds__extract_dow
            , subq_1.revenue_instance__ds__extract_doy AS revenue_instance__ds__extract_doy
            , subq_1.metric_time__month AS metric_time__month
            , subq_1.metric_time__quarter AS metric_time__quarter
            , subq_1.metric_time__year AS metric_time__year
            , subq_1.metric_time__extract_year AS metric_time__extract_year
            , subq_1.metric_time__extract_quarter AS metric_time__extract_quarter
            , subq_1.metric_time__extract_month AS metric_time__extract_month
            , subq_1.metric_time__extract_day AS metric_time__extract_day
            , subq_1.metric_time__extract_dow AS metric_time__extract_dow
            , subq_1.metric_time__extract_doy AS metric_time__extract_doy
            , subq_1.user AS user
            , subq_1.revenue_instance__user AS revenue_instance__user
            , subq_1.txn_revenue AS txn_revenue
          FROM (
            -- Time Spine
            SELECT
              subq_3.ds AS metric_time__day
              , DATETIME_TRUNC(subq_3.ds, isoweek) AS metric_time__week
            FROM ***************************.mf_time_spine subq_3
            GROUP BY
              metric_time__day
              , metric_time__week
          ) subq_2
          INNER JOIN (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_0.ds__day
              , subq_0.ds__week
              , subq_0.ds__month
              , subq_0.ds__quarter
              , subq_0.ds__year
              , subq_0.ds__extract_year
              , subq_0.ds__extract_quarter
              , subq_0.ds__extract_month
              , subq_0.ds__extract_day
              , subq_0.ds__extract_dow
              , subq_0.ds__extract_doy
              , subq_0.revenue_instance__ds__day
              , subq_0.revenue_instance__ds__week
              , subq_0.revenue_instance__ds__month
              , subq_0.revenue_instance__ds__quarter
              , subq_0.revenue_instance__ds__year
              , subq_0.revenue_instance__ds__extract_year
              , subq_0.revenue_instance__ds__extract_quarter
              , subq_0.revenue_instance__ds__extract_month
              , subq_0.revenue_instance__ds__extract_day
              , subq_0.revenue_instance__ds__extract_dow
              , subq_0.revenue_instance__ds__extract_doy
              , subq_0.ds__day AS metric_time__day
              , subq_0.ds__week AS metric_time__week
              , subq_0.ds__month AS metric_time__month
              , subq_0.ds__quarter AS metric_time__quarter
              , subq_0.ds__year AS metric_time__year
              , subq_0.ds__extract_year AS metric_time__extract_year
              , subq_0.ds__extract_quarter AS metric_time__extract_quarter
              , subq_0.ds__extract_month AS metric_time__extract_month
              , subq_0.ds__extract_day AS metric_time__extract_day
              , subq_0.ds__extract_dow AS metric_time__extract_dow
              , subq_0.ds__extract_doy AS metric_time__extract_doy
              , subq_0.user
              , subq_0.revenue_instance__user
              , subq_0.txn_revenue
            FROM (
              -- Read Elements From Semantic Model 'revenue'
              SELECT
                revenue_src_28000.revenue AS txn_revenue
                , DATETIME_TRUNC(revenue_src_28000.created_at, day) AS ds__day
                , DATETIME_TRUNC(revenue_src_28000.created_at, isoweek) AS ds__week
                , DATETIME_TRUNC(revenue_src_28000.created_at, month) AS ds__month
                , DATETIME_TRUNC(revenue_src_28000.created_at, quarter) AS ds__quarter
                , DATETIME_TRUNC(revenue_src_28000.created_at, year) AS ds__year
                , EXTRACT(year FROM revenue_src_28000.created_at) AS ds__extract_year
                , EXTRACT(quarter FROM revenue_src_28000.created_at) AS ds__extract_quarter
                , EXTRACT(month FROM revenue_src_28000.created_at) AS ds__extract_month
                , EXTRACT(day FROM revenue_src_28000.created_at) AS ds__extract_day
                , IF(EXTRACT(dayofweek FROM revenue_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM revenue_src_28000.created_at) - 1) AS ds__extract_dow
                , EXTRACT(dayofyear FROM revenue_src_28000.created_at) AS ds__extract_doy
                , DATETIME_TRUNC(revenue_src_28000.created_at, day) AS revenue_instance__ds__day
                , DATETIME_TRUNC(revenue_src_28000.created_at, isoweek) AS revenue_instance__ds__week
                , DATETIME_TRUNC(revenue_src_28000.created_at, month) AS revenue_instance__ds__month
                , DATETIME_TRUNC(revenue_src_28000.created_at, quarter) AS revenue_instance__ds__quarter
                , DATETIME_TRUNC(revenue_src_28000.created_at, year) AS revenue_instance__ds__year
                , EXTRACT(year FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_year
                , EXTRACT(quarter FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_quarter
                , EXTRACT(month FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_month
                , EXTRACT(day FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_day
                , IF(EXTRACT(dayofweek FROM revenue_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM revenue_src_28000.created_at) - 1) AS revenue_instance__ds__extract_dow
                , EXTRACT(dayofyear FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
                , revenue_src_28000.user_id AS user
                , revenue_src_28000.user_id AS revenue_instance__user
              FROM ***************************.fct_revenue revenue_src_28000
            ) subq_0
          ) subq_1
          ON
            (subq_1.metric_time__day <= subq_2.metric_time__day)
        ) subq_4
      ) subq_5
      GROUP BY
        metric_time__day
        , metric_time__week
    ) subq_6
  ) subq_7
) subq_8
GROUP BY
  metric_time__week
  , revenue_all_time
