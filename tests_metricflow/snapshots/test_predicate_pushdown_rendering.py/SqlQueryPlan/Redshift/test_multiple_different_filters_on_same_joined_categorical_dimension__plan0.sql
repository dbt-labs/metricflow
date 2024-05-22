-- Compute Metrics via Expressions
SELECT
  CAST(subq_28.west_coast_balance_first_day AS DOUBLE PRECISION) / CAST(NULLIF(subq_28.east_coast_balance_first_dat, 0) AS DOUBLE PRECISION) AS regional_starting_balance_ratios
FROM (
  -- Combine Aggregated Outputs
  SELECT
    MAX(subq_13.west_coast_balance_first_day) AS west_coast_balance_first_day
    , MAX(subq_27.east_coast_balance_first_dat) AS east_coast_balance_first_dat
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_12.total_account_balance_first_day AS west_coast_balance_first_day
    FROM (
      -- Aggregate Measures
      SELECT
        SUM(subq_11.total_account_balance_first_day) AS total_account_balance_first_day
      FROM (
        -- Pass Only Elements: ['total_account_balance_first_day',]
        SELECT
          subq_10.total_account_balance_first_day
        FROM (
          -- Join on MIN(ds) and [] grouping by None
          SELECT
            subq_7.ds__day AS ds__day
            , subq_7.user__home_state_latest AS user__home_state_latest
            , subq_7.total_account_balance_first_day AS total_account_balance_first_day
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_6.ds__day
              , subq_6.user__home_state_latest
              , subq_6.total_account_balance_first_day
            FROM (
              -- Pass Only Elements: ['total_account_balance_first_day', 'user__home_state_latest', 'ds__day']
              SELECT
                subq_5.ds__day
                , subq_5.user__home_state_latest
                , subq_5.total_account_balance_first_day
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_2.ds__day AS ds__day
                  , subq_2.user AS user
                  , subq_4.home_state_latest AS user__home_state_latest
                  , subq_2.total_account_balance_first_day AS total_account_balance_first_day
                FROM (
                  -- Pass Only Elements: ['total_account_balance_first_day', 'ds__day', 'user']
                  SELECT
                    subq_1.ds__day
                    , subq_1.user
                    , subq_1.total_account_balance_first_day
                  FROM (
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
                      , subq_0.account__ds__day
                      , subq_0.account__ds__week
                      , subq_0.account__ds__month
                      , subq_0.account__ds__quarter
                      , subq_0.account__ds__year
                      , subq_0.account__ds__extract_year
                      , subq_0.account__ds__extract_quarter
                      , subq_0.account__ds__extract_month
                      , subq_0.account__ds__extract_day
                      , subq_0.account__ds__extract_dow
                      , subq_0.account__ds__extract_doy
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
                      , subq_0.account__user
                      , subq_0.account_type
                      , subq_0.account__account_type
                      , subq_0.account_balance
                      , subq_0.total_account_balance_first_day
                      , subq_0.current_account_balance_by_user
                    FROM (
                      -- Read Elements From Semantic Model 'accounts_source'
                      SELECT
                        accounts_source_src_28000.account_balance
                        , accounts_source_src_28000.account_balance AS total_account_balance_first_day
                        , accounts_source_src_28000.account_balance AS current_account_balance_by_user
                        , DATE_TRUNC('day', accounts_source_src_28000.ds) AS ds__day
                        , DATE_TRUNC('week', accounts_source_src_28000.ds) AS ds__week
                        , DATE_TRUNC('month', accounts_source_src_28000.ds) AS ds__month
                        , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS ds__quarter
                        , DATE_TRUNC('year', accounts_source_src_28000.ds) AS ds__year
                        , EXTRACT(year FROM accounts_source_src_28000.ds) AS ds__extract_year
                        , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS ds__extract_quarter
                        , EXTRACT(month FROM accounts_source_src_28000.ds) AS ds__extract_month
                        , EXTRACT(day FROM accounts_source_src_28000.ds) AS ds__extract_day
                        , CASE WHEN EXTRACT(dow FROM accounts_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM accounts_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM accounts_source_src_28000.ds) END AS ds__extract_dow
                        , EXTRACT(doy FROM accounts_source_src_28000.ds) AS ds__extract_doy
                        , accounts_source_src_28000.account_type
                        , DATE_TRUNC('day', accounts_source_src_28000.ds) AS account__ds__day
                        , DATE_TRUNC('week', accounts_source_src_28000.ds) AS account__ds__week
                        , DATE_TRUNC('month', accounts_source_src_28000.ds) AS account__ds__month
                        , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS account__ds__quarter
                        , DATE_TRUNC('year', accounts_source_src_28000.ds) AS account__ds__year
                        , EXTRACT(year FROM accounts_source_src_28000.ds) AS account__ds__extract_year
                        , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS account__ds__extract_quarter
                        , EXTRACT(month FROM accounts_source_src_28000.ds) AS account__ds__extract_month
                        , EXTRACT(day FROM accounts_source_src_28000.ds) AS account__ds__extract_day
                        , CASE WHEN EXTRACT(dow FROM accounts_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM accounts_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM accounts_source_src_28000.ds) END AS account__ds__extract_dow
                        , EXTRACT(doy FROM accounts_source_src_28000.ds) AS account__ds__extract_doy
                        , accounts_source_src_28000.account_type AS account__account_type
                        , accounts_source_src_28000.user_id AS user
                        , accounts_source_src_28000.user_id AS account__user
                      FROM ***************************.fct_accounts accounts_source_src_28000
                    ) subq_0
                  ) subq_1
                ) subq_2
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['home_state_latest', 'user']
                  SELECT
                    subq_3.user
                    , subq_3.home_state_latest
                  FROM (
                    -- Read Elements From Semantic Model 'users_latest'
                    SELECT
                      DATE_TRUNC('day', users_latest_src_28000.ds) AS ds_latest__day
                      , DATE_TRUNC('week', users_latest_src_28000.ds) AS ds_latest__week
                      , DATE_TRUNC('month', users_latest_src_28000.ds) AS ds_latest__month
                      , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS ds_latest__quarter
                      , DATE_TRUNC('year', users_latest_src_28000.ds) AS ds_latest__year
                      , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
                      , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
                      , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
                      , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
                      , CASE WHEN EXTRACT(dow FROM users_latest_src_28000.ds) = 0 THEN EXTRACT(dow FROM users_latest_src_28000.ds) + 7 ELSE EXTRACT(dow FROM users_latest_src_28000.ds) END AS ds_latest__extract_dow
                      , EXTRACT(doy FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
                      , users_latest_src_28000.home_state_latest
                      , DATE_TRUNC('day', users_latest_src_28000.ds) AS user__ds_latest__day
                      , DATE_TRUNC('week', users_latest_src_28000.ds) AS user__ds_latest__week
                      , DATE_TRUNC('month', users_latest_src_28000.ds) AS user__ds_latest__month
                      , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS user__ds_latest__quarter
                      , DATE_TRUNC('year', users_latest_src_28000.ds) AS user__ds_latest__year
                      , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
                      , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
                      , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
                      , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
                      , CASE WHEN EXTRACT(dow FROM users_latest_src_28000.ds) = 0 THEN EXTRACT(dow FROM users_latest_src_28000.ds) + 7 ELSE EXTRACT(dow FROM users_latest_src_28000.ds) END AS user__ds_latest__extract_dow
                      , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                      , users_latest_src_28000.home_state_latest AS user__home_state_latest
                      , users_latest_src_28000.user_id AS user
                    FROM ***************************.dim_users_latest users_latest_src_28000
                  ) subq_3
                ) subq_4
                ON
                  subq_2.user = subq_4.user
              ) subq_5
            ) subq_6
            WHERE user__home_state_latest IN ('CA', 'HI', 'WA')
          ) subq_7
          INNER JOIN (
            -- Filter row on MIN(ds__day)
            SELECT
              MIN(subq_8.ds__day) AS ds__day__complete
            FROM (
              -- Constrain Output with WHERE
              SELECT
                subq_6.ds__day
                , subq_6.user__home_state_latest
                , subq_6.total_account_balance_first_day
              FROM (
                -- Pass Only Elements: ['total_account_balance_first_day', 'user__home_state_latest', 'ds__day']
                SELECT
                  subq_5.ds__day
                  , subq_5.user__home_state_latest
                  , subq_5.total_account_balance_first_day
                FROM (
                  -- Join Standard Outputs
                  SELECT
                    subq_2.ds__day AS ds__day
                    , subq_2.user AS user
                    , subq_4.home_state_latest AS user__home_state_latest
                    , subq_2.total_account_balance_first_day AS total_account_balance_first_day
                  FROM (
                    -- Pass Only Elements: ['total_account_balance_first_day', 'ds__day', 'user']
                    SELECT
                      subq_1.ds__day
                      , subq_1.user
                      , subq_1.total_account_balance_first_day
                    FROM (
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
                        , subq_0.account__ds__day
                        , subq_0.account__ds__week
                        , subq_0.account__ds__month
                        , subq_0.account__ds__quarter
                        , subq_0.account__ds__year
                        , subq_0.account__ds__extract_year
                        , subq_0.account__ds__extract_quarter
                        , subq_0.account__ds__extract_month
                        , subq_0.account__ds__extract_day
                        , subq_0.account__ds__extract_dow
                        , subq_0.account__ds__extract_doy
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
                        , subq_0.account__user
                        , subq_0.account_type
                        , subq_0.account__account_type
                        , subq_0.account_balance
                        , subq_0.total_account_balance_first_day
                        , subq_0.current_account_balance_by_user
                      FROM (
                        -- Read Elements From Semantic Model 'accounts_source'
                        SELECT
                          accounts_source_src_28000.account_balance
                          , accounts_source_src_28000.account_balance AS total_account_balance_first_day
                          , accounts_source_src_28000.account_balance AS current_account_balance_by_user
                          , DATE_TRUNC('day', accounts_source_src_28000.ds) AS ds__day
                          , DATE_TRUNC('week', accounts_source_src_28000.ds) AS ds__week
                          , DATE_TRUNC('month', accounts_source_src_28000.ds) AS ds__month
                          , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS ds__quarter
                          , DATE_TRUNC('year', accounts_source_src_28000.ds) AS ds__year
                          , EXTRACT(year FROM accounts_source_src_28000.ds) AS ds__extract_year
                          , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS ds__extract_quarter
                          , EXTRACT(month FROM accounts_source_src_28000.ds) AS ds__extract_month
                          , EXTRACT(day FROM accounts_source_src_28000.ds) AS ds__extract_day
                          , CASE WHEN EXTRACT(dow FROM accounts_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM accounts_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM accounts_source_src_28000.ds) END AS ds__extract_dow
                          , EXTRACT(doy FROM accounts_source_src_28000.ds) AS ds__extract_doy
                          , accounts_source_src_28000.account_type
                          , DATE_TRUNC('day', accounts_source_src_28000.ds) AS account__ds__day
                          , DATE_TRUNC('week', accounts_source_src_28000.ds) AS account__ds__week
                          , DATE_TRUNC('month', accounts_source_src_28000.ds) AS account__ds__month
                          , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS account__ds__quarter
                          , DATE_TRUNC('year', accounts_source_src_28000.ds) AS account__ds__year
                          , EXTRACT(year FROM accounts_source_src_28000.ds) AS account__ds__extract_year
                          , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS account__ds__extract_quarter
                          , EXTRACT(month FROM accounts_source_src_28000.ds) AS account__ds__extract_month
                          , EXTRACT(day FROM accounts_source_src_28000.ds) AS account__ds__extract_day
                          , CASE WHEN EXTRACT(dow FROM accounts_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM accounts_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM accounts_source_src_28000.ds) END AS account__ds__extract_dow
                          , EXTRACT(doy FROM accounts_source_src_28000.ds) AS account__ds__extract_doy
                          , accounts_source_src_28000.account_type AS account__account_type
                          , accounts_source_src_28000.user_id AS user
                          , accounts_source_src_28000.user_id AS account__user
                        FROM ***************************.fct_accounts accounts_source_src_28000
                      ) subq_0
                    ) subq_1
                  ) subq_2
                  LEFT OUTER JOIN (
                    -- Pass Only Elements: ['home_state_latest', 'user']
                    SELECT
                      subq_3.user
                      , subq_3.home_state_latest
                    FROM (
                      -- Read Elements From Semantic Model 'users_latest'
                      SELECT
                        DATE_TRUNC('day', users_latest_src_28000.ds) AS ds_latest__day
                        , DATE_TRUNC('week', users_latest_src_28000.ds) AS ds_latest__week
                        , DATE_TRUNC('month', users_latest_src_28000.ds) AS ds_latest__month
                        , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS ds_latest__quarter
                        , DATE_TRUNC('year', users_latest_src_28000.ds) AS ds_latest__year
                        , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
                        , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
                        , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
                        , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
                        , CASE WHEN EXTRACT(dow FROM users_latest_src_28000.ds) = 0 THEN EXTRACT(dow FROM users_latest_src_28000.ds) + 7 ELSE EXTRACT(dow FROM users_latest_src_28000.ds) END AS ds_latest__extract_dow
                        , EXTRACT(doy FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
                        , users_latest_src_28000.home_state_latest
                        , DATE_TRUNC('day', users_latest_src_28000.ds) AS user__ds_latest__day
                        , DATE_TRUNC('week', users_latest_src_28000.ds) AS user__ds_latest__week
                        , DATE_TRUNC('month', users_latest_src_28000.ds) AS user__ds_latest__month
                        , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS user__ds_latest__quarter
                        , DATE_TRUNC('year', users_latest_src_28000.ds) AS user__ds_latest__year
                        , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
                        , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
                        , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
                        , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
                        , CASE WHEN EXTRACT(dow FROM users_latest_src_28000.ds) = 0 THEN EXTRACT(dow FROM users_latest_src_28000.ds) + 7 ELSE EXTRACT(dow FROM users_latest_src_28000.ds) END AS user__ds_latest__extract_dow
                        , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                        , users_latest_src_28000.home_state_latest AS user__home_state_latest
                        , users_latest_src_28000.user_id AS user
                      FROM ***************************.dim_users_latest users_latest_src_28000
                    ) subq_3
                  ) subq_4
                  ON
                    subq_2.user = subq_4.user
                ) subq_5
              ) subq_6
              WHERE user__home_state_latest IN ('CA', 'HI', 'WA')
            ) subq_8
          ) subq_9
          ON
            subq_7.ds__day = subq_9.ds__day__complete
        ) subq_10
      ) subq_11
    ) subq_12
  ) subq_13
  CROSS JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_26.total_account_balance_first_day AS east_coast_balance_first_dat
    FROM (
      -- Aggregate Measures
      SELECT
        SUM(subq_25.total_account_balance_first_day) AS total_account_balance_first_day
      FROM (
        -- Pass Only Elements: ['total_account_balance_first_day',]
        SELECT
          subq_24.total_account_balance_first_day
        FROM (
          -- Join on MIN(ds) and [] grouping by None
          SELECT
            subq_21.ds__day AS ds__day
            , subq_21.user__home_state_latest AS user__home_state_latest
            , subq_21.total_account_balance_first_day AS total_account_balance_first_day
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_20.ds__day
              , subq_20.user__home_state_latest
              , subq_20.total_account_balance_first_day
            FROM (
              -- Pass Only Elements: ['total_account_balance_first_day', 'user__home_state_latest', 'ds__day']
              SELECT
                subq_19.ds__day
                , subq_19.user__home_state_latest
                , subq_19.total_account_balance_first_day
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_16.ds__day AS ds__day
                  , subq_16.user AS user
                  , subq_18.home_state_latest AS user__home_state_latest
                  , subq_16.total_account_balance_first_day AS total_account_balance_first_day
                FROM (
                  -- Pass Only Elements: ['total_account_balance_first_day', 'ds__day', 'user']
                  SELECT
                    subq_15.ds__day
                    , subq_15.user
                    , subq_15.total_account_balance_first_day
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_14.ds__day
                      , subq_14.ds__week
                      , subq_14.ds__month
                      , subq_14.ds__quarter
                      , subq_14.ds__year
                      , subq_14.ds__extract_year
                      , subq_14.ds__extract_quarter
                      , subq_14.ds__extract_month
                      , subq_14.ds__extract_day
                      , subq_14.ds__extract_dow
                      , subq_14.ds__extract_doy
                      , subq_14.account__ds__day
                      , subq_14.account__ds__week
                      , subq_14.account__ds__month
                      , subq_14.account__ds__quarter
                      , subq_14.account__ds__year
                      , subq_14.account__ds__extract_year
                      , subq_14.account__ds__extract_quarter
                      , subq_14.account__ds__extract_month
                      , subq_14.account__ds__extract_day
                      , subq_14.account__ds__extract_dow
                      , subq_14.account__ds__extract_doy
                      , subq_14.ds__day AS metric_time__day
                      , subq_14.ds__week AS metric_time__week
                      , subq_14.ds__month AS metric_time__month
                      , subq_14.ds__quarter AS metric_time__quarter
                      , subq_14.ds__year AS metric_time__year
                      , subq_14.ds__extract_year AS metric_time__extract_year
                      , subq_14.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_14.ds__extract_month AS metric_time__extract_month
                      , subq_14.ds__extract_day AS metric_time__extract_day
                      , subq_14.ds__extract_dow AS metric_time__extract_dow
                      , subq_14.ds__extract_doy AS metric_time__extract_doy
                      , subq_14.user
                      , subq_14.account__user
                      , subq_14.account_type
                      , subq_14.account__account_type
                      , subq_14.account_balance
                      , subq_14.total_account_balance_first_day
                      , subq_14.current_account_balance_by_user
                    FROM (
                      -- Read Elements From Semantic Model 'accounts_source'
                      SELECT
                        accounts_source_src_28000.account_balance
                        , accounts_source_src_28000.account_balance AS total_account_balance_first_day
                        , accounts_source_src_28000.account_balance AS current_account_balance_by_user
                        , DATE_TRUNC('day', accounts_source_src_28000.ds) AS ds__day
                        , DATE_TRUNC('week', accounts_source_src_28000.ds) AS ds__week
                        , DATE_TRUNC('month', accounts_source_src_28000.ds) AS ds__month
                        , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS ds__quarter
                        , DATE_TRUNC('year', accounts_source_src_28000.ds) AS ds__year
                        , EXTRACT(year FROM accounts_source_src_28000.ds) AS ds__extract_year
                        , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS ds__extract_quarter
                        , EXTRACT(month FROM accounts_source_src_28000.ds) AS ds__extract_month
                        , EXTRACT(day FROM accounts_source_src_28000.ds) AS ds__extract_day
                        , CASE WHEN EXTRACT(dow FROM accounts_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM accounts_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM accounts_source_src_28000.ds) END AS ds__extract_dow
                        , EXTRACT(doy FROM accounts_source_src_28000.ds) AS ds__extract_doy
                        , accounts_source_src_28000.account_type
                        , DATE_TRUNC('day', accounts_source_src_28000.ds) AS account__ds__day
                        , DATE_TRUNC('week', accounts_source_src_28000.ds) AS account__ds__week
                        , DATE_TRUNC('month', accounts_source_src_28000.ds) AS account__ds__month
                        , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS account__ds__quarter
                        , DATE_TRUNC('year', accounts_source_src_28000.ds) AS account__ds__year
                        , EXTRACT(year FROM accounts_source_src_28000.ds) AS account__ds__extract_year
                        , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS account__ds__extract_quarter
                        , EXTRACT(month FROM accounts_source_src_28000.ds) AS account__ds__extract_month
                        , EXTRACT(day FROM accounts_source_src_28000.ds) AS account__ds__extract_day
                        , CASE WHEN EXTRACT(dow FROM accounts_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM accounts_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM accounts_source_src_28000.ds) END AS account__ds__extract_dow
                        , EXTRACT(doy FROM accounts_source_src_28000.ds) AS account__ds__extract_doy
                        , accounts_source_src_28000.account_type AS account__account_type
                        , accounts_source_src_28000.user_id AS user
                        , accounts_source_src_28000.user_id AS account__user
                      FROM ***************************.fct_accounts accounts_source_src_28000
                    ) subq_14
                  ) subq_15
                ) subq_16
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['home_state_latest', 'user']
                  SELECT
                    subq_17.user
                    , subq_17.home_state_latest
                  FROM (
                    -- Read Elements From Semantic Model 'users_latest'
                    SELECT
                      DATE_TRUNC('day', users_latest_src_28000.ds) AS ds_latest__day
                      , DATE_TRUNC('week', users_latest_src_28000.ds) AS ds_latest__week
                      , DATE_TRUNC('month', users_latest_src_28000.ds) AS ds_latest__month
                      , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS ds_latest__quarter
                      , DATE_TRUNC('year', users_latest_src_28000.ds) AS ds_latest__year
                      , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
                      , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
                      , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
                      , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
                      , CASE WHEN EXTRACT(dow FROM users_latest_src_28000.ds) = 0 THEN EXTRACT(dow FROM users_latest_src_28000.ds) + 7 ELSE EXTRACT(dow FROM users_latest_src_28000.ds) END AS ds_latest__extract_dow
                      , EXTRACT(doy FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
                      , users_latest_src_28000.home_state_latest
                      , DATE_TRUNC('day', users_latest_src_28000.ds) AS user__ds_latest__day
                      , DATE_TRUNC('week', users_latest_src_28000.ds) AS user__ds_latest__week
                      , DATE_TRUNC('month', users_latest_src_28000.ds) AS user__ds_latest__month
                      , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS user__ds_latest__quarter
                      , DATE_TRUNC('year', users_latest_src_28000.ds) AS user__ds_latest__year
                      , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
                      , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
                      , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
                      , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
                      , CASE WHEN EXTRACT(dow FROM users_latest_src_28000.ds) = 0 THEN EXTRACT(dow FROM users_latest_src_28000.ds) + 7 ELSE EXTRACT(dow FROM users_latest_src_28000.ds) END AS user__ds_latest__extract_dow
                      , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                      , users_latest_src_28000.home_state_latest AS user__home_state_latest
                      , users_latest_src_28000.user_id AS user
                    FROM ***************************.dim_users_latest users_latest_src_28000
                  ) subq_17
                ) subq_18
                ON
                  subq_16.user = subq_18.user
              ) subq_19
            ) subq_20
            WHERE user__home_state_latest IN ('MD', 'NY', 'TX')
          ) subq_21
          INNER JOIN (
            -- Filter row on MIN(ds__day)
            SELECT
              MIN(subq_22.ds__day) AS ds__day__complete
            FROM (
              -- Constrain Output with WHERE
              SELECT
                subq_20.ds__day
                , subq_20.user__home_state_latest
                , subq_20.total_account_balance_first_day
              FROM (
                -- Pass Only Elements: ['total_account_balance_first_day', 'user__home_state_latest', 'ds__day']
                SELECT
                  subq_19.ds__day
                  , subq_19.user__home_state_latest
                  , subq_19.total_account_balance_first_day
                FROM (
                  -- Join Standard Outputs
                  SELECT
                    subq_16.ds__day AS ds__day
                    , subq_16.user AS user
                    , subq_18.home_state_latest AS user__home_state_latest
                    , subq_16.total_account_balance_first_day AS total_account_balance_first_day
                  FROM (
                    -- Pass Only Elements: ['total_account_balance_first_day', 'ds__day', 'user']
                    SELECT
                      subq_15.ds__day
                      , subq_15.user
                      , subq_15.total_account_balance_first_day
                    FROM (
                      -- Metric Time Dimension 'ds'
                      SELECT
                        subq_14.ds__day
                        , subq_14.ds__week
                        , subq_14.ds__month
                        , subq_14.ds__quarter
                        , subq_14.ds__year
                        , subq_14.ds__extract_year
                        , subq_14.ds__extract_quarter
                        , subq_14.ds__extract_month
                        , subq_14.ds__extract_day
                        , subq_14.ds__extract_dow
                        , subq_14.ds__extract_doy
                        , subq_14.account__ds__day
                        , subq_14.account__ds__week
                        , subq_14.account__ds__month
                        , subq_14.account__ds__quarter
                        , subq_14.account__ds__year
                        , subq_14.account__ds__extract_year
                        , subq_14.account__ds__extract_quarter
                        , subq_14.account__ds__extract_month
                        , subq_14.account__ds__extract_day
                        , subq_14.account__ds__extract_dow
                        , subq_14.account__ds__extract_doy
                        , subq_14.ds__day AS metric_time__day
                        , subq_14.ds__week AS metric_time__week
                        , subq_14.ds__month AS metric_time__month
                        , subq_14.ds__quarter AS metric_time__quarter
                        , subq_14.ds__year AS metric_time__year
                        , subq_14.ds__extract_year AS metric_time__extract_year
                        , subq_14.ds__extract_quarter AS metric_time__extract_quarter
                        , subq_14.ds__extract_month AS metric_time__extract_month
                        , subq_14.ds__extract_day AS metric_time__extract_day
                        , subq_14.ds__extract_dow AS metric_time__extract_dow
                        , subq_14.ds__extract_doy AS metric_time__extract_doy
                        , subq_14.user
                        , subq_14.account__user
                        , subq_14.account_type
                        , subq_14.account__account_type
                        , subq_14.account_balance
                        , subq_14.total_account_balance_first_day
                        , subq_14.current_account_balance_by_user
                      FROM (
                        -- Read Elements From Semantic Model 'accounts_source'
                        SELECT
                          accounts_source_src_28000.account_balance
                          , accounts_source_src_28000.account_balance AS total_account_balance_first_day
                          , accounts_source_src_28000.account_balance AS current_account_balance_by_user
                          , DATE_TRUNC('day', accounts_source_src_28000.ds) AS ds__day
                          , DATE_TRUNC('week', accounts_source_src_28000.ds) AS ds__week
                          , DATE_TRUNC('month', accounts_source_src_28000.ds) AS ds__month
                          , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS ds__quarter
                          , DATE_TRUNC('year', accounts_source_src_28000.ds) AS ds__year
                          , EXTRACT(year FROM accounts_source_src_28000.ds) AS ds__extract_year
                          , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS ds__extract_quarter
                          , EXTRACT(month FROM accounts_source_src_28000.ds) AS ds__extract_month
                          , EXTRACT(day FROM accounts_source_src_28000.ds) AS ds__extract_day
                          , CASE WHEN EXTRACT(dow FROM accounts_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM accounts_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM accounts_source_src_28000.ds) END AS ds__extract_dow
                          , EXTRACT(doy FROM accounts_source_src_28000.ds) AS ds__extract_doy
                          , accounts_source_src_28000.account_type
                          , DATE_TRUNC('day', accounts_source_src_28000.ds) AS account__ds__day
                          , DATE_TRUNC('week', accounts_source_src_28000.ds) AS account__ds__week
                          , DATE_TRUNC('month', accounts_source_src_28000.ds) AS account__ds__month
                          , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS account__ds__quarter
                          , DATE_TRUNC('year', accounts_source_src_28000.ds) AS account__ds__year
                          , EXTRACT(year FROM accounts_source_src_28000.ds) AS account__ds__extract_year
                          , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS account__ds__extract_quarter
                          , EXTRACT(month FROM accounts_source_src_28000.ds) AS account__ds__extract_month
                          , EXTRACT(day FROM accounts_source_src_28000.ds) AS account__ds__extract_day
                          , CASE WHEN EXTRACT(dow FROM accounts_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM accounts_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM accounts_source_src_28000.ds) END AS account__ds__extract_dow
                          , EXTRACT(doy FROM accounts_source_src_28000.ds) AS account__ds__extract_doy
                          , accounts_source_src_28000.account_type AS account__account_type
                          , accounts_source_src_28000.user_id AS user
                          , accounts_source_src_28000.user_id AS account__user
                        FROM ***************************.fct_accounts accounts_source_src_28000
                      ) subq_14
                    ) subq_15
                  ) subq_16
                  LEFT OUTER JOIN (
                    -- Pass Only Elements: ['home_state_latest', 'user']
                    SELECT
                      subq_17.user
                      , subq_17.home_state_latest
                    FROM (
                      -- Read Elements From Semantic Model 'users_latest'
                      SELECT
                        DATE_TRUNC('day', users_latest_src_28000.ds) AS ds_latest__day
                        , DATE_TRUNC('week', users_latest_src_28000.ds) AS ds_latest__week
                        , DATE_TRUNC('month', users_latest_src_28000.ds) AS ds_latest__month
                        , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS ds_latest__quarter
                        , DATE_TRUNC('year', users_latest_src_28000.ds) AS ds_latest__year
                        , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
                        , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
                        , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
                        , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
                        , CASE WHEN EXTRACT(dow FROM users_latest_src_28000.ds) = 0 THEN EXTRACT(dow FROM users_latest_src_28000.ds) + 7 ELSE EXTRACT(dow FROM users_latest_src_28000.ds) END AS ds_latest__extract_dow
                        , EXTRACT(doy FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
                        , users_latest_src_28000.home_state_latest
                        , DATE_TRUNC('day', users_latest_src_28000.ds) AS user__ds_latest__day
                        , DATE_TRUNC('week', users_latest_src_28000.ds) AS user__ds_latest__week
                        , DATE_TRUNC('month', users_latest_src_28000.ds) AS user__ds_latest__month
                        , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS user__ds_latest__quarter
                        , DATE_TRUNC('year', users_latest_src_28000.ds) AS user__ds_latest__year
                        , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
                        , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
                        , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
                        , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
                        , CASE WHEN EXTRACT(dow FROM users_latest_src_28000.ds) = 0 THEN EXTRACT(dow FROM users_latest_src_28000.ds) + 7 ELSE EXTRACT(dow FROM users_latest_src_28000.ds) END AS user__ds_latest__extract_dow
                        , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                        , users_latest_src_28000.home_state_latest AS user__home_state_latest
                        , users_latest_src_28000.user_id AS user
                      FROM ***************************.dim_users_latest users_latest_src_28000
                    ) subq_17
                  ) subq_18
                  ON
                    subq_16.user = subq_18.user
                ) subq_19
              ) subq_20
              WHERE user__home_state_latest IN ('MD', 'NY', 'TX')
            ) subq_22
          ) subq_23
          ON
            subq_21.ds__day = subq_23.ds__day__complete
        ) subq_24
      ) subq_25
    ) subq_26
  ) subq_27
) subq_28
