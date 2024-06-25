-- Compute Metrics via Expressions
SELECT
  subq_22.third_hop_count
FROM (
  -- Aggregate Measures
  SELECT
    COUNT(DISTINCT subq_21.third_hop_count) AS third_hop_count
  FROM (
    -- Pass Only Elements: ['third_hop_count',]
    SELECT
      subq_20.third_hop_count
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_19.customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count
        , subq_19.third_hop_count
      FROM (
        -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count']
        SELECT
          subq_18.customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count
          , subq_18.third_hop_count
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_2.customer_third_hop_id AS customer_third_hop_id
            , subq_17.account_id__customer_id__customer_third_hop_id AS customer_third_hop_id__account_id__customer_id__customer_third_hop_id
            , subq_17.account_id__customer_id__customer_third_hop_id__txn_count AS customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count
            , subq_2.third_hop_count AS third_hop_count
          FROM (
            -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id']
            SELECT
              subq_1.customer_third_hop_id
              , subq_1.third_hop_count
            FROM (
              -- Metric Time Dimension 'third_hop_ds'
              SELECT
                subq_0.third_hop_ds__day
                , subq_0.third_hop_ds__week
                , subq_0.third_hop_ds__month
                , subq_0.third_hop_ds__quarter
                , subq_0.third_hop_ds__year
                , subq_0.third_hop_ds__extract_year
                , subq_0.third_hop_ds__extract_quarter
                , subq_0.third_hop_ds__extract_month
                , subq_0.third_hop_ds__extract_day
                , subq_0.third_hop_ds__extract_dow
                , subq_0.third_hop_ds__extract_doy
                , subq_0.customer_third_hop_id__third_hop_ds__day
                , subq_0.customer_third_hop_id__third_hop_ds__week
                , subq_0.customer_third_hop_id__third_hop_ds__month
                , subq_0.customer_third_hop_id__third_hop_ds__quarter
                , subq_0.customer_third_hop_id__third_hop_ds__year
                , subq_0.customer_third_hop_id__third_hop_ds__extract_year
                , subq_0.customer_third_hop_id__third_hop_ds__extract_quarter
                , subq_0.customer_third_hop_id__third_hop_ds__extract_month
                , subq_0.customer_third_hop_id__third_hop_ds__extract_day
                , subq_0.customer_third_hop_id__third_hop_ds__extract_dow
                , subq_0.customer_third_hop_id__third_hop_ds__extract_doy
                , subq_0.third_hop_ds__day AS metric_time__day
                , subq_0.third_hop_ds__week AS metric_time__week
                , subq_0.third_hop_ds__month AS metric_time__month
                , subq_0.third_hop_ds__quarter AS metric_time__quarter
                , subq_0.third_hop_ds__year AS metric_time__year
                , subq_0.third_hop_ds__extract_year AS metric_time__extract_year
                , subq_0.third_hop_ds__extract_quarter AS metric_time__extract_quarter
                , subq_0.third_hop_ds__extract_month AS metric_time__extract_month
                , subq_0.third_hop_ds__extract_day AS metric_time__extract_day
                , subq_0.third_hop_ds__extract_dow AS metric_time__extract_dow
                , subq_0.third_hop_ds__extract_doy AS metric_time__extract_doy
                , subq_0.customer_third_hop_id
                , subq_0.value
                , subq_0.customer_third_hop_id__value
                , subq_0.third_hop_count
              FROM (
                -- Read Elements From Semantic Model 'third_hop_table'
                SELECT
                  third_hop_table_src_22000.customer_third_hop_id AS third_hop_count
                  , third_hop_table_src_22000.value
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, day) AS third_hop_ds__day
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, isoweek) AS third_hop_ds__week
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, month) AS third_hop_ds__month
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, quarter) AS third_hop_ds__quarter
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, year) AS third_hop_ds__year
                  , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_year
                  , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_quarter
                  , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_month
                  , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_day
                  , IF(EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) = 1, 7, EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) - 1) AS third_hop_ds__extract_dow
                  , EXTRACT(dayofyear FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_doy
                  , third_hop_table_src_22000.value AS customer_third_hop_id__value
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, day) AS customer_third_hop_id__third_hop_ds__day
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, isoweek) AS customer_third_hop_id__third_hop_ds__week
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, month) AS customer_third_hop_id__third_hop_ds__month
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, quarter) AS customer_third_hop_id__third_hop_ds__quarter
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, year) AS customer_third_hop_id__third_hop_ds__year
                  , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_year
                  , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_quarter
                  , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_month
                  , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_day
                  , IF(EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) = 1, 7, EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) - 1) AS customer_third_hop_id__third_hop_ds__extract_dow
                  , EXTRACT(dayofyear FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_doy
                  , third_hop_table_src_22000.customer_third_hop_id
                FROM ***************************.third_hop_table third_hop_table_src_22000
              ) subq_0
            ) subq_1
          ) subq_2
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['account_id__customer_id__customer_third_hop_id', 'account_id__customer_id__customer_third_hop_id__txn_count']
            SELECT
              subq_16.account_id__customer_id__customer_third_hop_id
              , subq_16.account_id__customer_id__customer_third_hop_id__txn_count
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_15.account_id__customer_id__customer_third_hop_id
                , subq_15.txn_count AS account_id__customer_id__customer_third_hop_id__txn_count
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_14.account_id__customer_id__customer_third_hop_id
                  , SUM(subq_14.txn_count) AS txn_count
                FROM (
                  -- Pass Only Elements: ['txn_count', 'account_id__customer_id__customer_third_hop_id']
                  SELECT
                    subq_13.account_id__customer_id__customer_third_hop_id
                    , subq_13.txn_count
                  FROM (
                    -- Join Standard Outputs
                    SELECT
                      subq_5.ds_partitioned__day AS ds_partitioned__day
                      , subq_12.ds_partitioned__day AS account_id__ds_partitioned__day
                      , subq_5.account_id AS account_id
                      , subq_12.customer_id__customer_third_hop_id AS account_id__customer_id__customer_third_hop_id
                      , subq_5.txn_count AS txn_count
                    FROM (
                      -- Pass Only Elements: ['txn_count', 'ds_partitioned__day', 'account_id']
                      SELECT
                        subq_4.ds_partitioned__day
                        , subq_4.account_id
                        , subq_4.txn_count
                      FROM (
                        -- Metric Time Dimension 'ds'
                        SELECT
                          subq_3.ds_partitioned__day
                          , subq_3.ds_partitioned__week
                          , subq_3.ds_partitioned__month
                          , subq_3.ds_partitioned__quarter
                          , subq_3.ds_partitioned__year
                          , subq_3.ds_partitioned__extract_year
                          , subq_3.ds_partitioned__extract_quarter
                          , subq_3.ds_partitioned__extract_month
                          , subq_3.ds_partitioned__extract_day
                          , subq_3.ds_partitioned__extract_dow
                          , subq_3.ds_partitioned__extract_doy
                          , subq_3.ds__day
                          , subq_3.ds__week
                          , subq_3.ds__month
                          , subq_3.ds__quarter
                          , subq_3.ds__year
                          , subq_3.ds__extract_year
                          , subq_3.ds__extract_quarter
                          , subq_3.ds__extract_month
                          , subq_3.ds__extract_day
                          , subq_3.ds__extract_dow
                          , subq_3.ds__extract_doy
                          , subq_3.account_id__ds_partitioned__day
                          , subq_3.account_id__ds_partitioned__week
                          , subq_3.account_id__ds_partitioned__month
                          , subq_3.account_id__ds_partitioned__quarter
                          , subq_3.account_id__ds_partitioned__year
                          , subq_3.account_id__ds_partitioned__extract_year
                          , subq_3.account_id__ds_partitioned__extract_quarter
                          , subq_3.account_id__ds_partitioned__extract_month
                          , subq_3.account_id__ds_partitioned__extract_day
                          , subq_3.account_id__ds_partitioned__extract_dow
                          , subq_3.account_id__ds_partitioned__extract_doy
                          , subq_3.account_id__ds__day
                          , subq_3.account_id__ds__week
                          , subq_3.account_id__ds__month
                          , subq_3.account_id__ds__quarter
                          , subq_3.account_id__ds__year
                          , subq_3.account_id__ds__extract_year
                          , subq_3.account_id__ds__extract_quarter
                          , subq_3.account_id__ds__extract_month
                          , subq_3.account_id__ds__extract_day
                          , subq_3.account_id__ds__extract_dow
                          , subq_3.account_id__ds__extract_doy
                          , subq_3.ds__day AS metric_time__day
                          , subq_3.ds__week AS metric_time__week
                          , subq_3.ds__month AS metric_time__month
                          , subq_3.ds__quarter AS metric_time__quarter
                          , subq_3.ds__year AS metric_time__year
                          , subq_3.ds__extract_year AS metric_time__extract_year
                          , subq_3.ds__extract_quarter AS metric_time__extract_quarter
                          , subq_3.ds__extract_month AS metric_time__extract_month
                          , subq_3.ds__extract_day AS metric_time__extract_day
                          , subq_3.ds__extract_dow AS metric_time__extract_dow
                          , subq_3.ds__extract_doy AS metric_time__extract_doy
                          , subq_3.account_id
                          , subq_3.account_month
                          , subq_3.account_id__account_month
                          , subq_3.txn_count
                        FROM (
                          -- Read Elements From Semantic Model 'account_month_txns'
                          SELECT
                            account_month_txns_src_22000.txn_count
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds_partitioned, day) AS ds_partitioned__day
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds_partitioned, isoweek) AS ds_partitioned__week
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds_partitioned, month) AS ds_partitioned__month
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds_partitioned, quarter) AS ds_partitioned__quarter
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds_partitioned, year) AS ds_partitioned__year
                            , EXTRACT(year FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_year
                            , EXTRACT(quarter FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_quarter
                            , EXTRACT(month FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_month
                            , EXTRACT(day FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_day
                            , IF(EXTRACT(dayofweek FROM account_month_txns_src_22000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM account_month_txns_src_22000.ds_partitioned) - 1) AS ds_partitioned__extract_dow
                            , EXTRACT(dayofyear FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_doy
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds, day) AS ds__day
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds, isoweek) AS ds__week
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds, month) AS ds__month
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds, quarter) AS ds__quarter
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds, year) AS ds__year
                            , EXTRACT(year FROM account_month_txns_src_22000.ds) AS ds__extract_year
                            , EXTRACT(quarter FROM account_month_txns_src_22000.ds) AS ds__extract_quarter
                            , EXTRACT(month FROM account_month_txns_src_22000.ds) AS ds__extract_month
                            , EXTRACT(day FROM account_month_txns_src_22000.ds) AS ds__extract_day
                            , IF(EXTRACT(dayofweek FROM account_month_txns_src_22000.ds) = 1, 7, EXTRACT(dayofweek FROM account_month_txns_src_22000.ds) - 1) AS ds__extract_dow
                            , EXTRACT(dayofyear FROM account_month_txns_src_22000.ds) AS ds__extract_doy
                            , account_month_txns_src_22000.account_month
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds_partitioned, day) AS account_id__ds_partitioned__day
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds_partitioned, isoweek) AS account_id__ds_partitioned__week
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds_partitioned, month) AS account_id__ds_partitioned__month
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds_partitioned, quarter) AS account_id__ds_partitioned__quarter
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds_partitioned, year) AS account_id__ds_partitioned__year
                            , EXTRACT(year FROM account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_year
                            , EXTRACT(quarter FROM account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_quarter
                            , EXTRACT(month FROM account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_month
                            , EXTRACT(day FROM account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_day
                            , IF(EXTRACT(dayofweek FROM account_month_txns_src_22000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM account_month_txns_src_22000.ds_partitioned) - 1) AS account_id__ds_partitioned__extract_dow
                            , EXTRACT(dayofyear FROM account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_doy
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds, day) AS account_id__ds__day
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds, isoweek) AS account_id__ds__week
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds, month) AS account_id__ds__month
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds, quarter) AS account_id__ds__quarter
                            , DATETIME_TRUNC(account_month_txns_src_22000.ds, year) AS account_id__ds__year
                            , EXTRACT(year FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_year
                            , EXTRACT(quarter FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_quarter
                            , EXTRACT(month FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_month
                            , EXTRACT(day FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_day
                            , IF(EXTRACT(dayofweek FROM account_month_txns_src_22000.ds) = 1, 7, EXTRACT(dayofweek FROM account_month_txns_src_22000.ds) - 1) AS account_id__ds__extract_dow
                            , EXTRACT(dayofyear FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_doy
                            , account_month_txns_src_22000.account_month AS account_id__account_month
                            , account_month_txns_src_22000.account_id
                          FROM ***************************.account_month_txns account_month_txns_src_22000
                        ) subq_3
                      ) subq_4
                    ) subq_5
                    LEFT OUTER JOIN (
                      -- Pass Only Elements: ['ds_partitioned__day', 'account_id', 'customer_id__customer_third_hop_id']
                      SELECT
                        subq_11.ds_partitioned__day
                        , subq_11.account_id
                        , subq_11.customer_id__customer_third_hop_id
                      FROM (
                        -- Join Standard Outputs
                        SELECT
                          subq_7.ds_partitioned__day AS ds_partitioned__day
                          , subq_7.ds_partitioned__week AS ds_partitioned__week
                          , subq_7.ds_partitioned__month AS ds_partitioned__month
                          , subq_7.ds_partitioned__quarter AS ds_partitioned__quarter
                          , subq_7.ds_partitioned__year AS ds_partitioned__year
                          , subq_7.ds_partitioned__extract_year AS ds_partitioned__extract_year
                          , subq_7.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                          , subq_7.ds_partitioned__extract_month AS ds_partitioned__extract_month
                          , subq_7.ds_partitioned__extract_day AS ds_partitioned__extract_day
                          , subq_7.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                          , subq_7.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                          , subq_7.account_id__ds_partitioned__day AS account_id__ds_partitioned__day
                          , subq_7.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
                          , subq_7.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
                          , subq_7.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
                          , subq_7.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
                          , subq_7.account_id__ds_partitioned__extract_year AS account_id__ds_partitioned__extract_year
                          , subq_7.account_id__ds_partitioned__extract_quarter AS account_id__ds_partitioned__extract_quarter
                          , subq_7.account_id__ds_partitioned__extract_month AS account_id__ds_partitioned__extract_month
                          , subq_7.account_id__ds_partitioned__extract_day AS account_id__ds_partitioned__extract_day
                          , subq_7.account_id__ds_partitioned__extract_dow AS account_id__ds_partitioned__extract_dow
                          , subq_7.account_id__ds_partitioned__extract_doy AS account_id__ds_partitioned__extract_doy
                          , subq_7.bridge_account__ds_partitioned__day AS bridge_account__ds_partitioned__day
                          , subq_7.bridge_account__ds_partitioned__week AS bridge_account__ds_partitioned__week
                          , subq_7.bridge_account__ds_partitioned__month AS bridge_account__ds_partitioned__month
                          , subq_7.bridge_account__ds_partitioned__quarter AS bridge_account__ds_partitioned__quarter
                          , subq_7.bridge_account__ds_partitioned__year AS bridge_account__ds_partitioned__year
                          , subq_7.bridge_account__ds_partitioned__extract_year AS bridge_account__ds_partitioned__extract_year
                          , subq_7.bridge_account__ds_partitioned__extract_quarter AS bridge_account__ds_partitioned__extract_quarter
                          , subq_7.bridge_account__ds_partitioned__extract_month AS bridge_account__ds_partitioned__extract_month
                          , subq_7.bridge_account__ds_partitioned__extract_day AS bridge_account__ds_partitioned__extract_day
                          , subq_7.bridge_account__ds_partitioned__extract_dow AS bridge_account__ds_partitioned__extract_dow
                          , subq_7.bridge_account__ds_partitioned__extract_doy AS bridge_account__ds_partitioned__extract_doy
                          , subq_7.metric_time__day AS metric_time__day
                          , subq_7.metric_time__week AS metric_time__week
                          , subq_7.metric_time__month AS metric_time__month
                          , subq_7.metric_time__quarter AS metric_time__quarter
                          , subq_7.metric_time__year AS metric_time__year
                          , subq_7.metric_time__extract_year AS metric_time__extract_year
                          , subq_7.metric_time__extract_quarter AS metric_time__extract_quarter
                          , subq_7.metric_time__extract_month AS metric_time__extract_month
                          , subq_7.metric_time__extract_day AS metric_time__extract_day
                          , subq_7.metric_time__extract_dow AS metric_time__extract_dow
                          , subq_7.metric_time__extract_doy AS metric_time__extract_doy
                          , subq_10.acquired_ds__day AS customer_id__acquired_ds__day
                          , subq_10.acquired_ds__week AS customer_id__acquired_ds__week
                          , subq_10.acquired_ds__month AS customer_id__acquired_ds__month
                          , subq_10.acquired_ds__quarter AS customer_id__acquired_ds__quarter
                          , subq_10.acquired_ds__year AS customer_id__acquired_ds__year
                          , subq_10.acquired_ds__extract_year AS customer_id__acquired_ds__extract_year
                          , subq_10.acquired_ds__extract_quarter AS customer_id__acquired_ds__extract_quarter
                          , subq_10.acquired_ds__extract_month AS customer_id__acquired_ds__extract_month
                          , subq_10.acquired_ds__extract_day AS customer_id__acquired_ds__extract_day
                          , subq_10.acquired_ds__extract_dow AS customer_id__acquired_ds__extract_dow
                          , subq_10.acquired_ds__extract_doy AS customer_id__acquired_ds__extract_doy
                          , subq_10.customer_third_hop_id__acquired_ds__day AS customer_id__customer_third_hop_id__acquired_ds__day
                          , subq_10.customer_third_hop_id__acquired_ds__week AS customer_id__customer_third_hop_id__acquired_ds__week
                          , subq_10.customer_third_hop_id__acquired_ds__month AS customer_id__customer_third_hop_id__acquired_ds__month
                          , subq_10.customer_third_hop_id__acquired_ds__quarter AS customer_id__customer_third_hop_id__acquired_ds__quarter
                          , subq_10.customer_third_hop_id__acquired_ds__year AS customer_id__customer_third_hop_id__acquired_ds__year
                          , subq_10.customer_third_hop_id__acquired_ds__extract_year AS customer_id__customer_third_hop_id__acquired_ds__extract_year
                          , subq_10.customer_third_hop_id__acquired_ds__extract_quarter AS customer_id__customer_third_hop_id__acquired_ds__extract_quarter
                          , subq_10.customer_third_hop_id__acquired_ds__extract_month AS customer_id__customer_third_hop_id__acquired_ds__extract_month
                          , subq_10.customer_third_hop_id__acquired_ds__extract_day AS customer_id__customer_third_hop_id__acquired_ds__extract_day
                          , subq_10.customer_third_hop_id__acquired_ds__extract_dow AS customer_id__customer_third_hop_id__acquired_ds__extract_dow
                          , subq_10.customer_third_hop_id__acquired_ds__extract_doy AS customer_id__customer_third_hop_id__acquired_ds__extract_doy
                          , subq_10.metric_time__day AS customer_id__metric_time__day
                          , subq_10.metric_time__week AS customer_id__metric_time__week
                          , subq_10.metric_time__month AS customer_id__metric_time__month
                          , subq_10.metric_time__quarter AS customer_id__metric_time__quarter
                          , subq_10.metric_time__year AS customer_id__metric_time__year
                          , subq_10.metric_time__extract_year AS customer_id__metric_time__extract_year
                          , subq_10.metric_time__extract_quarter AS customer_id__metric_time__extract_quarter
                          , subq_10.metric_time__extract_month AS customer_id__metric_time__extract_month
                          , subq_10.metric_time__extract_day AS customer_id__metric_time__extract_day
                          , subq_10.metric_time__extract_dow AS customer_id__metric_time__extract_dow
                          , subq_10.metric_time__extract_doy AS customer_id__metric_time__extract_doy
                          , subq_7.account_id AS account_id
                          , subq_7.customer_id AS customer_id
                          , subq_7.account_id__customer_id AS account_id__customer_id
                          , subq_7.bridge_account__account_id AS bridge_account__account_id
                          , subq_7.bridge_account__customer_id AS bridge_account__customer_id
                          , subq_10.customer_third_hop_id AS customer_id__customer_third_hop_id
                          , subq_10.customer_third_hop_id__customer_id AS customer_id__customer_third_hop_id__customer_id
                          , subq_7.extra_dim AS extra_dim
                          , subq_7.account_id__extra_dim AS account_id__extra_dim
                          , subq_7.bridge_account__extra_dim AS bridge_account__extra_dim
                          , subq_10.country AS customer_id__country
                          , subq_10.customer_third_hop_id__country AS customer_id__customer_third_hop_id__country
                          , subq_7.account_customer_combos AS account_customer_combos
                        FROM (
                          -- Metric Time Dimension 'ds_partitioned'
                          SELECT
                            subq_6.ds_partitioned__day
                            , subq_6.ds_partitioned__week
                            , subq_6.ds_partitioned__month
                            , subq_6.ds_partitioned__quarter
                            , subq_6.ds_partitioned__year
                            , subq_6.ds_partitioned__extract_year
                            , subq_6.ds_partitioned__extract_quarter
                            , subq_6.ds_partitioned__extract_month
                            , subq_6.ds_partitioned__extract_day
                            , subq_6.ds_partitioned__extract_dow
                            , subq_6.ds_partitioned__extract_doy
                            , subq_6.account_id__ds_partitioned__day
                            , subq_6.account_id__ds_partitioned__week
                            , subq_6.account_id__ds_partitioned__month
                            , subq_6.account_id__ds_partitioned__quarter
                            , subq_6.account_id__ds_partitioned__year
                            , subq_6.account_id__ds_partitioned__extract_year
                            , subq_6.account_id__ds_partitioned__extract_quarter
                            , subq_6.account_id__ds_partitioned__extract_month
                            , subq_6.account_id__ds_partitioned__extract_day
                            , subq_6.account_id__ds_partitioned__extract_dow
                            , subq_6.account_id__ds_partitioned__extract_doy
                            , subq_6.bridge_account__ds_partitioned__day
                            , subq_6.bridge_account__ds_partitioned__week
                            , subq_6.bridge_account__ds_partitioned__month
                            , subq_6.bridge_account__ds_partitioned__quarter
                            , subq_6.bridge_account__ds_partitioned__year
                            , subq_6.bridge_account__ds_partitioned__extract_year
                            , subq_6.bridge_account__ds_partitioned__extract_quarter
                            , subq_6.bridge_account__ds_partitioned__extract_month
                            , subq_6.bridge_account__ds_partitioned__extract_day
                            , subq_6.bridge_account__ds_partitioned__extract_dow
                            , subq_6.bridge_account__ds_partitioned__extract_doy
                            , subq_6.ds_partitioned__day AS metric_time__day
                            , subq_6.ds_partitioned__week AS metric_time__week
                            , subq_6.ds_partitioned__month AS metric_time__month
                            , subq_6.ds_partitioned__quarter AS metric_time__quarter
                            , subq_6.ds_partitioned__year AS metric_time__year
                            , subq_6.ds_partitioned__extract_year AS metric_time__extract_year
                            , subq_6.ds_partitioned__extract_quarter AS metric_time__extract_quarter
                            , subq_6.ds_partitioned__extract_month AS metric_time__extract_month
                            , subq_6.ds_partitioned__extract_day AS metric_time__extract_day
                            , subq_6.ds_partitioned__extract_dow AS metric_time__extract_dow
                            , subq_6.ds_partitioned__extract_doy AS metric_time__extract_doy
                            , subq_6.account_id
                            , subq_6.customer_id
                            , subq_6.account_id__customer_id
                            , subq_6.bridge_account__account_id
                            , subq_6.bridge_account__customer_id
                            , subq_6.extra_dim
                            , subq_6.account_id__extra_dim
                            , subq_6.bridge_account__extra_dim
                            , subq_6.account_customer_combos
                          FROM (
                            -- Read Elements From Semantic Model 'bridge_table'
                            SELECT
                              account_id || customer_id AS account_customer_combos
                              , bridge_table_src_22000.extra_dim
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, day) AS ds_partitioned__day
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, isoweek) AS ds_partitioned__week
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, month) AS ds_partitioned__month
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, quarter) AS ds_partitioned__quarter
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, year) AS ds_partitioned__year
                              , EXTRACT(year FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_year
                              , EXTRACT(quarter FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_quarter
                              , EXTRACT(month FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_month
                              , EXTRACT(day FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_day
                              , IF(EXTRACT(dayofweek FROM bridge_table_src_22000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bridge_table_src_22000.ds_partitioned) - 1) AS ds_partitioned__extract_dow
                              , EXTRACT(dayofyear FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_doy
                              , bridge_table_src_22000.extra_dim AS account_id__extra_dim
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, day) AS account_id__ds_partitioned__day
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, isoweek) AS account_id__ds_partitioned__week
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, month) AS account_id__ds_partitioned__month
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, quarter) AS account_id__ds_partitioned__quarter
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, year) AS account_id__ds_partitioned__year
                              , EXTRACT(year FROM bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_year
                              , EXTRACT(quarter FROM bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_quarter
                              , EXTRACT(month FROM bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_month
                              , EXTRACT(day FROM bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_day
                              , IF(EXTRACT(dayofweek FROM bridge_table_src_22000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bridge_table_src_22000.ds_partitioned) - 1) AS account_id__ds_partitioned__extract_dow
                              , EXTRACT(dayofyear FROM bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_doy
                              , bridge_table_src_22000.extra_dim AS bridge_account__extra_dim
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, day) AS bridge_account__ds_partitioned__day
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, isoweek) AS bridge_account__ds_partitioned__week
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, month) AS bridge_account__ds_partitioned__month
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, quarter) AS bridge_account__ds_partitioned__quarter
                              , DATETIME_TRUNC(bridge_table_src_22000.ds_partitioned, year) AS bridge_account__ds_partitioned__year
                              , EXTRACT(year FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_year
                              , EXTRACT(quarter FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_quarter
                              , EXTRACT(month FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_month
                              , EXTRACT(day FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_day
                              , IF(EXTRACT(dayofweek FROM bridge_table_src_22000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bridge_table_src_22000.ds_partitioned) - 1) AS bridge_account__ds_partitioned__extract_dow
                              , EXTRACT(dayofyear FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_doy
                              , bridge_table_src_22000.account_id
                              , bridge_table_src_22000.customer_id
                              , bridge_table_src_22000.customer_id AS account_id__customer_id
                              , bridge_table_src_22000.account_id AS bridge_account__account_id
                              , bridge_table_src_22000.customer_id AS bridge_account__customer_id
                            FROM ***************************.bridge_table bridge_table_src_22000
                          ) subq_6
                        ) subq_7
                        LEFT OUTER JOIN (
                          -- Pass Only Elements: [
                          --   'country',
                          --   'customer_id__country',
                          --   'customer_third_hop_id__country',
                          --   'acquired_ds__day',
                          --   'acquired_ds__week',
                          --   'acquired_ds__month',
                          --   'acquired_ds__quarter',
                          --   'acquired_ds__year',
                          --   'acquired_ds__extract_year',
                          --   'acquired_ds__extract_quarter',
                          --   'acquired_ds__extract_month',
                          --   'acquired_ds__extract_day',
                          --   'acquired_ds__extract_dow',
                          --   'acquired_ds__extract_doy',
                          --   'customer_id__acquired_ds__day',
                          --   'customer_id__acquired_ds__week',
                          --   'customer_id__acquired_ds__month',
                          --   'customer_id__acquired_ds__quarter',
                          --   'customer_id__acquired_ds__year',
                          --   'customer_id__acquired_ds__extract_year',
                          --   'customer_id__acquired_ds__extract_quarter',
                          --   'customer_id__acquired_ds__extract_month',
                          --   'customer_id__acquired_ds__extract_day',
                          --   'customer_id__acquired_ds__extract_dow',
                          --   'customer_id__acquired_ds__extract_doy',
                          --   'customer_third_hop_id__acquired_ds__day',
                          --   'customer_third_hop_id__acquired_ds__week',
                          --   'customer_third_hop_id__acquired_ds__month',
                          --   'customer_third_hop_id__acquired_ds__quarter',
                          --   'customer_third_hop_id__acquired_ds__year',
                          --   'customer_third_hop_id__acquired_ds__extract_year',
                          --   'customer_third_hop_id__acquired_ds__extract_quarter',
                          --   'customer_third_hop_id__acquired_ds__extract_month',
                          --   'customer_third_hop_id__acquired_ds__extract_day',
                          --   'customer_third_hop_id__acquired_ds__extract_dow',
                          --   'customer_third_hop_id__acquired_ds__extract_doy',
                          --   'metric_time__day',
                          --   'metric_time__week',
                          --   'metric_time__month',
                          --   'metric_time__quarter',
                          --   'metric_time__year',
                          --   'metric_time__extract_year',
                          --   'metric_time__extract_quarter',
                          --   'metric_time__extract_month',
                          --   'metric_time__extract_day',
                          --   'metric_time__extract_dow',
                          --   'metric_time__extract_doy',
                          --   'customer_id',
                          --   'customer_third_hop_id',
                          --   'customer_id__customer_third_hop_id',
                          --   'customer_third_hop_id__customer_id',
                          -- ]
                          SELECT
                            subq_9.acquired_ds__day
                            , subq_9.acquired_ds__week
                            , subq_9.acquired_ds__month
                            , subq_9.acquired_ds__quarter
                            , subq_9.acquired_ds__year
                            , subq_9.acquired_ds__extract_year
                            , subq_9.acquired_ds__extract_quarter
                            , subq_9.acquired_ds__extract_month
                            , subq_9.acquired_ds__extract_day
                            , subq_9.acquired_ds__extract_dow
                            , subq_9.acquired_ds__extract_doy
                            , subq_9.customer_id__acquired_ds__day
                            , subq_9.customer_id__acquired_ds__week
                            , subq_9.customer_id__acquired_ds__month
                            , subq_9.customer_id__acquired_ds__quarter
                            , subq_9.customer_id__acquired_ds__year
                            , subq_9.customer_id__acquired_ds__extract_year
                            , subq_9.customer_id__acquired_ds__extract_quarter
                            , subq_9.customer_id__acquired_ds__extract_month
                            , subq_9.customer_id__acquired_ds__extract_day
                            , subq_9.customer_id__acquired_ds__extract_dow
                            , subq_9.customer_id__acquired_ds__extract_doy
                            , subq_9.customer_third_hop_id__acquired_ds__day
                            , subq_9.customer_third_hop_id__acquired_ds__week
                            , subq_9.customer_third_hop_id__acquired_ds__month
                            , subq_9.customer_third_hop_id__acquired_ds__quarter
                            , subq_9.customer_third_hop_id__acquired_ds__year
                            , subq_9.customer_third_hop_id__acquired_ds__extract_year
                            , subq_9.customer_third_hop_id__acquired_ds__extract_quarter
                            , subq_9.customer_third_hop_id__acquired_ds__extract_month
                            , subq_9.customer_third_hop_id__acquired_ds__extract_day
                            , subq_9.customer_third_hop_id__acquired_ds__extract_dow
                            , subq_9.customer_third_hop_id__acquired_ds__extract_doy
                            , subq_9.metric_time__day
                            , subq_9.metric_time__week
                            , subq_9.metric_time__month
                            , subq_9.metric_time__quarter
                            , subq_9.metric_time__year
                            , subq_9.metric_time__extract_year
                            , subq_9.metric_time__extract_quarter
                            , subq_9.metric_time__extract_month
                            , subq_9.metric_time__extract_day
                            , subq_9.metric_time__extract_dow
                            , subq_9.metric_time__extract_doy
                            , subq_9.customer_id
                            , subq_9.customer_third_hop_id
                            , subq_9.customer_id__customer_third_hop_id
                            , subq_9.customer_third_hop_id__customer_id
                            , subq_9.country
                            , subq_9.customer_id__country
                            , subq_9.customer_third_hop_id__country
                          FROM (
                            -- Metric Time Dimension 'acquired_ds'
                            SELECT
                              subq_8.acquired_ds__day
                              , subq_8.acquired_ds__week
                              , subq_8.acquired_ds__month
                              , subq_8.acquired_ds__quarter
                              , subq_8.acquired_ds__year
                              , subq_8.acquired_ds__extract_year
                              , subq_8.acquired_ds__extract_quarter
                              , subq_8.acquired_ds__extract_month
                              , subq_8.acquired_ds__extract_day
                              , subq_8.acquired_ds__extract_dow
                              , subq_8.acquired_ds__extract_doy
                              , subq_8.customer_id__acquired_ds__day
                              , subq_8.customer_id__acquired_ds__week
                              , subq_8.customer_id__acquired_ds__month
                              , subq_8.customer_id__acquired_ds__quarter
                              , subq_8.customer_id__acquired_ds__year
                              , subq_8.customer_id__acquired_ds__extract_year
                              , subq_8.customer_id__acquired_ds__extract_quarter
                              , subq_8.customer_id__acquired_ds__extract_month
                              , subq_8.customer_id__acquired_ds__extract_day
                              , subq_8.customer_id__acquired_ds__extract_dow
                              , subq_8.customer_id__acquired_ds__extract_doy
                              , subq_8.customer_third_hop_id__acquired_ds__day
                              , subq_8.customer_third_hop_id__acquired_ds__week
                              , subq_8.customer_third_hop_id__acquired_ds__month
                              , subq_8.customer_third_hop_id__acquired_ds__quarter
                              , subq_8.customer_third_hop_id__acquired_ds__year
                              , subq_8.customer_third_hop_id__acquired_ds__extract_year
                              , subq_8.customer_third_hop_id__acquired_ds__extract_quarter
                              , subq_8.customer_third_hop_id__acquired_ds__extract_month
                              , subq_8.customer_third_hop_id__acquired_ds__extract_day
                              , subq_8.customer_third_hop_id__acquired_ds__extract_dow
                              , subq_8.customer_third_hop_id__acquired_ds__extract_doy
                              , subq_8.acquired_ds__day AS metric_time__day
                              , subq_8.acquired_ds__week AS metric_time__week
                              , subq_8.acquired_ds__month AS metric_time__month
                              , subq_8.acquired_ds__quarter AS metric_time__quarter
                              , subq_8.acquired_ds__year AS metric_time__year
                              , subq_8.acquired_ds__extract_year AS metric_time__extract_year
                              , subq_8.acquired_ds__extract_quarter AS metric_time__extract_quarter
                              , subq_8.acquired_ds__extract_month AS metric_time__extract_month
                              , subq_8.acquired_ds__extract_day AS metric_time__extract_day
                              , subq_8.acquired_ds__extract_dow AS metric_time__extract_dow
                              , subq_8.acquired_ds__extract_doy AS metric_time__extract_doy
                              , subq_8.customer_id
                              , subq_8.customer_third_hop_id
                              , subq_8.customer_id__customer_third_hop_id
                              , subq_8.customer_third_hop_id__customer_id
                              , subq_8.country
                              , subq_8.customer_id__country
                              , subq_8.customer_third_hop_id__country
                              , subq_8.customers_with_other_data
                            FROM (
                              -- Read Elements From Semantic Model 'customer_other_data'
                              SELECT
                                1 AS customers_with_other_data
                                , customer_other_data_src_22000.country
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, day) AS acquired_ds__day
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, isoweek) AS acquired_ds__week
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, month) AS acquired_ds__month
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, quarter) AS acquired_ds__quarter
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, year) AS acquired_ds__year
                                , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_year
                                , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_quarter
                                , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_month
                                , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_day
                                , IF(EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) = 1, 7, EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) - 1) AS acquired_ds__extract_dow
                                , EXTRACT(dayofyear FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_doy
                                , customer_other_data_src_22000.country AS customer_id__country
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, day) AS customer_id__acquired_ds__day
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, isoweek) AS customer_id__acquired_ds__week
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, month) AS customer_id__acquired_ds__month
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, quarter) AS customer_id__acquired_ds__quarter
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, year) AS customer_id__acquired_ds__year
                                , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_year
                                , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_quarter
                                , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_month
                                , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_day
                                , IF(EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) = 1, 7, EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) - 1) AS customer_id__acquired_ds__extract_dow
                                , EXTRACT(dayofyear FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_doy
                                , customer_other_data_src_22000.country AS customer_third_hop_id__country
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, day) AS customer_third_hop_id__acquired_ds__day
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, isoweek) AS customer_third_hop_id__acquired_ds__week
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, month) AS customer_third_hop_id__acquired_ds__month
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, quarter) AS customer_third_hop_id__acquired_ds__quarter
                                , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, year) AS customer_third_hop_id__acquired_ds__year
                                , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_year
                                , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_quarter
                                , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_month
                                , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_day
                                , IF(EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) = 1, 7, EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) - 1) AS customer_third_hop_id__acquired_ds__extract_dow
                                , EXTRACT(dayofyear FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_doy
                                , customer_other_data_src_22000.customer_id
                                , customer_other_data_src_22000.customer_third_hop_id
                                , customer_other_data_src_22000.customer_third_hop_id AS customer_id__customer_third_hop_id
                                , customer_other_data_src_22000.customer_id AS customer_third_hop_id__customer_id
                              FROM ***************************.customer_other_data customer_other_data_src_22000
                            ) subq_8
                          ) subq_9
                        ) subq_10
                        ON
                          subq_7.customer_id = subq_10.customer_id
                      ) subq_11
                    ) subq_12
                    ON
                      (
                        subq_5.account_id = subq_12.account_id
                      ) AND (
                        subq_5.ds_partitioned__day = subq_12.ds_partitioned__day
                      )
                  ) subq_13
                ) subq_14
                GROUP BY
                  account_id__customer_id__customer_third_hop_id
              ) subq_15
            ) subq_16
          ) subq_17
          ON
            subq_2.customer_third_hop_id = subq_17.account_id__customer_id__customer_third_hop_id
        ) subq_18
      ) subq_19
      WHERE customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count > 2
    ) subq_20
  ) subq_21
) subq_22
