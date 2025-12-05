test_name: test_inner_query_multi_hop
test_filename: test_metric_filter_rendering.py
docstring:
  Tests rendering for a metric filter using a two-hop join in the inner query.
sql_engine: Trino
---
-- Write to DataTable
SELECT
  subq_32.third_hop_count
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_31.__third_hop_count AS third_hop_count
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      COUNT(DISTINCT subq_30.__third_hop_count) AS __third_hop_count
    FROM (
      -- Pass Only Elements: ['__third_hop_count']
      SELECT
        subq_29.__third_hop_count
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_28.third_hop_count AS __third_hop_count
          , subq_28.customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count
        FROM (
          -- Pass Only Elements: ['__third_hop_count', 'customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count']
          SELECT
            subq_27.customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count
            , subq_27.__third_hop_count AS third_hop_count
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_26.account_id__customer_id__customer_third_hop_id AS customer_third_hop_id__account_id__customer_id__customer_third_hop_id
              , subq_26.account_id__customer_id__customer_third_hop_id__txn_count AS customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count
              , subq_11.third_hop_ds__day AS third_hop_ds__day
              , subq_11.third_hop_ds__week AS third_hop_ds__week
              , subq_11.third_hop_ds__month AS third_hop_ds__month
              , subq_11.third_hop_ds__quarter AS third_hop_ds__quarter
              , subq_11.third_hop_ds__year AS third_hop_ds__year
              , subq_11.third_hop_ds__extract_year AS third_hop_ds__extract_year
              , subq_11.third_hop_ds__extract_quarter AS third_hop_ds__extract_quarter
              , subq_11.third_hop_ds__extract_month AS third_hop_ds__extract_month
              , subq_11.third_hop_ds__extract_day AS third_hop_ds__extract_day
              , subq_11.third_hop_ds__extract_dow AS third_hop_ds__extract_dow
              , subq_11.third_hop_ds__extract_doy AS third_hop_ds__extract_doy
              , subq_11.customer_third_hop_id__third_hop_ds__day AS customer_third_hop_id__third_hop_ds__day
              , subq_11.customer_third_hop_id__third_hop_ds__week AS customer_third_hop_id__third_hop_ds__week
              , subq_11.customer_third_hop_id__third_hop_ds__month AS customer_third_hop_id__third_hop_ds__month
              , subq_11.customer_third_hop_id__third_hop_ds__quarter AS customer_third_hop_id__third_hop_ds__quarter
              , subq_11.customer_third_hop_id__third_hop_ds__year AS customer_third_hop_id__third_hop_ds__year
              , subq_11.customer_third_hop_id__third_hop_ds__extract_year AS customer_third_hop_id__third_hop_ds__extract_year
              , subq_11.customer_third_hop_id__third_hop_ds__extract_quarter AS customer_third_hop_id__third_hop_ds__extract_quarter
              , subq_11.customer_third_hop_id__third_hop_ds__extract_month AS customer_third_hop_id__third_hop_ds__extract_month
              , subq_11.customer_third_hop_id__third_hop_ds__extract_day AS customer_third_hop_id__third_hop_ds__extract_day
              , subq_11.customer_third_hop_id__third_hop_ds__extract_dow AS customer_third_hop_id__third_hop_ds__extract_dow
              , subq_11.customer_third_hop_id__third_hop_ds__extract_doy AS customer_third_hop_id__third_hop_ds__extract_doy
              , subq_11.metric_time__day AS metric_time__day
              , subq_11.metric_time__week AS metric_time__week
              , subq_11.metric_time__month AS metric_time__month
              , subq_11.metric_time__quarter AS metric_time__quarter
              , subq_11.metric_time__year AS metric_time__year
              , subq_11.metric_time__extract_year AS metric_time__extract_year
              , subq_11.metric_time__extract_quarter AS metric_time__extract_quarter
              , subq_11.metric_time__extract_month AS metric_time__extract_month
              , subq_11.metric_time__extract_day AS metric_time__extract_day
              , subq_11.metric_time__extract_dow AS metric_time__extract_dow
              , subq_11.metric_time__extract_doy AS metric_time__extract_doy
              , subq_11.customer_third_hop_id AS customer_third_hop_id
              , subq_11.value AS value
              , subq_11.customer_third_hop_id__value AS customer_third_hop_id__value
              , subq_11.__third_hop_count AS __third_hop_count
            FROM (
              -- Metric Time Dimension 'third_hop_ds'
              SELECT
                subq_10.third_hop_ds__day
                , subq_10.third_hop_ds__week
                , subq_10.third_hop_ds__month
                , subq_10.third_hop_ds__quarter
                , subq_10.third_hop_ds__year
                , subq_10.third_hop_ds__extract_year
                , subq_10.third_hop_ds__extract_quarter
                , subq_10.third_hop_ds__extract_month
                , subq_10.third_hop_ds__extract_day
                , subq_10.third_hop_ds__extract_dow
                , subq_10.third_hop_ds__extract_doy
                , subq_10.customer_third_hop_id__third_hop_ds__day
                , subq_10.customer_third_hop_id__third_hop_ds__week
                , subq_10.customer_third_hop_id__third_hop_ds__month
                , subq_10.customer_third_hop_id__third_hop_ds__quarter
                , subq_10.customer_third_hop_id__third_hop_ds__year
                , subq_10.customer_third_hop_id__third_hop_ds__extract_year
                , subq_10.customer_third_hop_id__third_hop_ds__extract_quarter
                , subq_10.customer_third_hop_id__third_hop_ds__extract_month
                , subq_10.customer_third_hop_id__third_hop_ds__extract_day
                , subq_10.customer_third_hop_id__third_hop_ds__extract_dow
                , subq_10.customer_third_hop_id__third_hop_ds__extract_doy
                , subq_10.third_hop_ds__day AS metric_time__day
                , subq_10.third_hop_ds__week AS metric_time__week
                , subq_10.third_hop_ds__month AS metric_time__month
                , subq_10.third_hop_ds__quarter AS metric_time__quarter
                , subq_10.third_hop_ds__year AS metric_time__year
                , subq_10.third_hop_ds__extract_year AS metric_time__extract_year
                , subq_10.third_hop_ds__extract_quarter AS metric_time__extract_quarter
                , subq_10.third_hop_ds__extract_month AS metric_time__extract_month
                , subq_10.third_hop_ds__extract_day AS metric_time__extract_day
                , subq_10.third_hop_ds__extract_dow AS metric_time__extract_dow
                , subq_10.third_hop_ds__extract_doy AS metric_time__extract_doy
                , subq_10.customer_third_hop_id
                , subq_10.value
                , subq_10.customer_third_hop_id__value
                , subq_10.__third_hop_count
              FROM (
                -- Read Elements From Semantic Model 'third_hop_table'
                SELECT
                  third_hop_table_src_22000.customer_third_hop_id AS __third_hop_count
                  , third_hop_table_src_22000.value
                  , DATE_TRUNC('day', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__day
                  , DATE_TRUNC('week', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__week
                  , DATE_TRUNC('month', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__month
                  , DATE_TRUNC('quarter', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__quarter
                  , DATE_TRUNC('year', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__year
                  , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_year
                  , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_quarter
                  , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_month
                  , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_day
                  , EXTRACT(DAY_OF_WEEK FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_dow
                  , EXTRACT(doy FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_doy
                  , third_hop_table_src_22000.value AS customer_third_hop_id__value
                  , DATE_TRUNC('day', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__day
                  , DATE_TRUNC('week', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__week
                  , DATE_TRUNC('month', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__month
                  , DATE_TRUNC('quarter', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__quarter
                  , DATE_TRUNC('year', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__year
                  , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_year
                  , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_quarter
                  , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_month
                  , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_day
                  , EXTRACT(DAY_OF_WEEK FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_dow
                  , EXTRACT(doy FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_doy
                  , third_hop_table_src_22000.customer_third_hop_id
                FROM ***************************.third_hop_table third_hop_table_src_22000
              ) subq_10
            ) subq_11
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['account_id__customer_id__customer_third_hop_id', 'account_id__customer_id__customer_third_hop_id__txn_count']
              SELECT
                subq_25.account_id__customer_id__customer_third_hop_id
                , subq_25.account_id__customer_id__customer_third_hop_id__txn_count
              FROM (
                -- Compute Metrics via Expressions
                SELECT
                  subq_24.account_id__customer_id__customer_third_hop_id
                  , subq_24.__txn_count AS account_id__customer_id__customer_third_hop_id__txn_count
                FROM (
                  -- Aggregate Inputs for Simple Metrics
                  SELECT
                    subq_23.account_id__customer_id__customer_third_hop_id
                    , SUM(subq_23.__txn_count) AS __txn_count
                  FROM (
                    -- Pass Only Elements: ['__txn_count', 'account_id__customer_id__customer_third_hop_id']
                    SELECT
                      subq_22.account_id__customer_id__customer_third_hop_id
                      , subq_22.__txn_count
                    FROM (
                      -- Pass Only Elements: ['__txn_count', 'account_id__customer_id__customer_third_hop_id']
                      SELECT
                        subq_21.account_id__customer_id__customer_third_hop_id
                        , subq_21.__txn_count
                      FROM (
                        -- Join Standard Outputs
                        SELECT
                          subq_20.ds_partitioned__day AS account_id__ds_partitioned__day
                          , subq_20.customer_id__customer_third_hop_id AS account_id__customer_id__customer_third_hop_id
                          , subq_13.ds_partitioned__day AS ds_partitioned__day
                          , subq_13.ds_partitioned__week AS ds_partitioned__week
                          , subq_13.ds_partitioned__month AS ds_partitioned__month
                          , subq_13.ds_partitioned__quarter AS ds_partitioned__quarter
                          , subq_13.ds_partitioned__year AS ds_partitioned__year
                          , subq_13.ds_partitioned__extract_year AS ds_partitioned__extract_year
                          , subq_13.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                          , subq_13.ds_partitioned__extract_month AS ds_partitioned__extract_month
                          , subq_13.ds_partitioned__extract_day AS ds_partitioned__extract_day
                          , subq_13.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                          , subq_13.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                          , subq_13.ds__day AS ds__day
                          , subq_13.ds__week AS ds__week
                          , subq_13.ds__month AS ds__month
                          , subq_13.ds__quarter AS ds__quarter
                          , subq_13.ds__year AS ds__year
                          , subq_13.ds__extract_year AS ds__extract_year
                          , subq_13.ds__extract_quarter AS ds__extract_quarter
                          , subq_13.ds__extract_month AS ds__extract_month
                          , subq_13.ds__extract_day AS ds__extract_day
                          , subq_13.ds__extract_dow AS ds__extract_dow
                          , subq_13.ds__extract_doy AS ds__extract_doy
                          , subq_13.account_id__ds_partitioned__day AS account_id__ds_partitioned__day
                          , subq_13.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
                          , subq_13.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
                          , subq_13.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
                          , subq_13.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
                          , subq_13.account_id__ds_partitioned__extract_year AS account_id__ds_partitioned__extract_year
                          , subq_13.account_id__ds_partitioned__extract_quarter AS account_id__ds_partitioned__extract_quarter
                          , subq_13.account_id__ds_partitioned__extract_month AS account_id__ds_partitioned__extract_month
                          , subq_13.account_id__ds_partitioned__extract_day AS account_id__ds_partitioned__extract_day
                          , subq_13.account_id__ds_partitioned__extract_dow AS account_id__ds_partitioned__extract_dow
                          , subq_13.account_id__ds_partitioned__extract_doy AS account_id__ds_partitioned__extract_doy
                          , subq_13.account_id__ds__day AS account_id__ds__day
                          , subq_13.account_id__ds__week AS account_id__ds__week
                          , subq_13.account_id__ds__month AS account_id__ds__month
                          , subq_13.account_id__ds__quarter AS account_id__ds__quarter
                          , subq_13.account_id__ds__year AS account_id__ds__year
                          , subq_13.account_id__ds__extract_year AS account_id__ds__extract_year
                          , subq_13.account_id__ds__extract_quarter AS account_id__ds__extract_quarter
                          , subq_13.account_id__ds__extract_month AS account_id__ds__extract_month
                          , subq_13.account_id__ds__extract_day AS account_id__ds__extract_day
                          , subq_13.account_id__ds__extract_dow AS account_id__ds__extract_dow
                          , subq_13.account_id__ds__extract_doy AS account_id__ds__extract_doy
                          , subq_13.metric_time__day AS metric_time__day
                          , subq_13.metric_time__week AS metric_time__week
                          , subq_13.metric_time__month AS metric_time__month
                          , subq_13.metric_time__quarter AS metric_time__quarter
                          , subq_13.metric_time__year AS metric_time__year
                          , subq_13.metric_time__extract_year AS metric_time__extract_year
                          , subq_13.metric_time__extract_quarter AS metric_time__extract_quarter
                          , subq_13.metric_time__extract_month AS metric_time__extract_month
                          , subq_13.metric_time__extract_day AS metric_time__extract_day
                          , subq_13.metric_time__extract_dow AS metric_time__extract_dow
                          , subq_13.metric_time__extract_doy AS metric_time__extract_doy
                          , subq_13.account_id AS account_id
                          , subq_13.account_month AS account_month
                          , subq_13.account_id__account_month AS account_id__account_month
                          , subq_13.__txn_count AS __txn_count
                        FROM (
                          -- Metric Time Dimension 'ds'
                          SELECT
                            subq_12.ds_partitioned__day
                            , subq_12.ds_partitioned__week
                            , subq_12.ds_partitioned__month
                            , subq_12.ds_partitioned__quarter
                            , subq_12.ds_partitioned__year
                            , subq_12.ds_partitioned__extract_year
                            , subq_12.ds_partitioned__extract_quarter
                            , subq_12.ds_partitioned__extract_month
                            , subq_12.ds_partitioned__extract_day
                            , subq_12.ds_partitioned__extract_dow
                            , subq_12.ds_partitioned__extract_doy
                            , subq_12.ds__day
                            , subq_12.ds__week
                            , subq_12.ds__month
                            , subq_12.ds__quarter
                            , subq_12.ds__year
                            , subq_12.ds__extract_year
                            , subq_12.ds__extract_quarter
                            , subq_12.ds__extract_month
                            , subq_12.ds__extract_day
                            , subq_12.ds__extract_dow
                            , subq_12.ds__extract_doy
                            , subq_12.account_id__ds_partitioned__day
                            , subq_12.account_id__ds_partitioned__week
                            , subq_12.account_id__ds_partitioned__month
                            , subq_12.account_id__ds_partitioned__quarter
                            , subq_12.account_id__ds_partitioned__year
                            , subq_12.account_id__ds_partitioned__extract_year
                            , subq_12.account_id__ds_partitioned__extract_quarter
                            , subq_12.account_id__ds_partitioned__extract_month
                            , subq_12.account_id__ds_partitioned__extract_day
                            , subq_12.account_id__ds_partitioned__extract_dow
                            , subq_12.account_id__ds_partitioned__extract_doy
                            , subq_12.account_id__ds__day
                            , subq_12.account_id__ds__week
                            , subq_12.account_id__ds__month
                            , subq_12.account_id__ds__quarter
                            , subq_12.account_id__ds__year
                            , subq_12.account_id__ds__extract_year
                            , subq_12.account_id__ds__extract_quarter
                            , subq_12.account_id__ds__extract_month
                            , subq_12.account_id__ds__extract_day
                            , subq_12.account_id__ds__extract_dow
                            , subq_12.account_id__ds__extract_doy
                            , subq_12.ds__day AS metric_time__day
                            , subq_12.ds__week AS metric_time__week
                            , subq_12.ds__month AS metric_time__month
                            , subq_12.ds__quarter AS metric_time__quarter
                            , subq_12.ds__year AS metric_time__year
                            , subq_12.ds__extract_year AS metric_time__extract_year
                            , subq_12.ds__extract_quarter AS metric_time__extract_quarter
                            , subq_12.ds__extract_month AS metric_time__extract_month
                            , subq_12.ds__extract_day AS metric_time__extract_day
                            , subq_12.ds__extract_dow AS metric_time__extract_dow
                            , subq_12.ds__extract_doy AS metric_time__extract_doy
                            , subq_12.account_id
                            , subq_12.account_month
                            , subq_12.account_id__account_month
                            , subq_12.__txn_count
                          FROM (
                            -- Read Elements From Semantic Model 'account_month_txns'
                            SELECT
                              account_month_txns_src_22000.txn_count AS __txn_count
                              , DATE_TRUNC('day', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__day
                              , DATE_TRUNC('week', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__week
                              , DATE_TRUNC('month', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__month
                              , DATE_TRUNC('quarter', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__quarter
                              , DATE_TRUNC('year', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__year
                              , EXTRACT(year FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_year
                              , EXTRACT(quarter FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_quarter
                              , EXTRACT(month FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_month
                              , EXTRACT(day FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_day
                              , EXTRACT(DAY_OF_WEEK FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_dow
                              , EXTRACT(doy FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_doy
                              , DATE_TRUNC('day', account_month_txns_src_22000.ds) AS ds__day
                              , DATE_TRUNC('week', account_month_txns_src_22000.ds) AS ds__week
                              , DATE_TRUNC('month', account_month_txns_src_22000.ds) AS ds__month
                              , DATE_TRUNC('quarter', account_month_txns_src_22000.ds) AS ds__quarter
                              , DATE_TRUNC('year', account_month_txns_src_22000.ds) AS ds__year
                              , EXTRACT(year FROM account_month_txns_src_22000.ds) AS ds__extract_year
                              , EXTRACT(quarter FROM account_month_txns_src_22000.ds) AS ds__extract_quarter
                              , EXTRACT(month FROM account_month_txns_src_22000.ds) AS ds__extract_month
                              , EXTRACT(day FROM account_month_txns_src_22000.ds) AS ds__extract_day
                              , EXTRACT(DAY_OF_WEEK FROM account_month_txns_src_22000.ds) AS ds__extract_dow
                              , EXTRACT(doy FROM account_month_txns_src_22000.ds) AS ds__extract_doy
                              , account_month_txns_src_22000.account_month
                              , DATE_TRUNC('day', account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__day
                              , DATE_TRUNC('week', account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__week
                              , DATE_TRUNC('month', account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__month
                              , DATE_TRUNC('quarter', account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__quarter
                              , DATE_TRUNC('year', account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__year
                              , EXTRACT(year FROM account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_year
                              , EXTRACT(quarter FROM account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_quarter
                              , EXTRACT(month FROM account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_month
                              , EXTRACT(day FROM account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_day
                              , EXTRACT(DAY_OF_WEEK FROM account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_dow
                              , EXTRACT(doy FROM account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_doy
                              , DATE_TRUNC('day', account_month_txns_src_22000.ds) AS account_id__ds__day
                              , DATE_TRUNC('week', account_month_txns_src_22000.ds) AS account_id__ds__week
                              , DATE_TRUNC('month', account_month_txns_src_22000.ds) AS account_id__ds__month
                              , DATE_TRUNC('quarter', account_month_txns_src_22000.ds) AS account_id__ds__quarter
                              , DATE_TRUNC('year', account_month_txns_src_22000.ds) AS account_id__ds__year
                              , EXTRACT(year FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_year
                              , EXTRACT(quarter FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_quarter
                              , EXTRACT(month FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_month
                              , EXTRACT(day FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_day
                              , EXTRACT(DAY_OF_WEEK FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_dow
                              , EXTRACT(doy FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_doy
                              , account_month_txns_src_22000.account_month AS account_id__account_month
                              , account_month_txns_src_22000.account_id
                            FROM ***************************.account_month_txns account_month_txns_src_22000
                          ) subq_12
                        ) subq_13
                        LEFT OUTER JOIN (
                          -- Pass Only Elements: ['ds_partitioned__day', 'account_id', 'customer_id__customer_third_hop_id']
                          SELECT
                            subq_19.ds_partitioned__day
                            , subq_19.account_id
                            , subq_19.customer_id__customer_third_hop_id
                          FROM (
                            -- Join Standard Outputs
                            SELECT
                              subq_18.country AS customer_id__country
                              , subq_18.customer_third_hop_id__country AS customer_id__customer_third_hop_id__country
                              , subq_18.acquired_ds__day AS customer_id__acquired_ds__day
                              , subq_18.acquired_ds__week AS customer_id__acquired_ds__week
                              , subq_18.acquired_ds__month AS customer_id__acquired_ds__month
                              , subq_18.acquired_ds__quarter AS customer_id__acquired_ds__quarter
                              , subq_18.acquired_ds__year AS customer_id__acquired_ds__year
                              , subq_18.acquired_ds__extract_year AS customer_id__acquired_ds__extract_year
                              , subq_18.acquired_ds__extract_quarter AS customer_id__acquired_ds__extract_quarter
                              , subq_18.acquired_ds__extract_month AS customer_id__acquired_ds__extract_month
                              , subq_18.acquired_ds__extract_day AS customer_id__acquired_ds__extract_day
                              , subq_18.acquired_ds__extract_dow AS customer_id__acquired_ds__extract_dow
                              , subq_18.acquired_ds__extract_doy AS customer_id__acquired_ds__extract_doy
                              , subq_18.customer_third_hop_id__acquired_ds__day AS customer_id__customer_third_hop_id__acquired_ds__day
                              , subq_18.customer_third_hop_id__acquired_ds__week AS customer_id__customer_third_hop_id__acquired_ds__week
                              , subq_18.customer_third_hop_id__acquired_ds__month AS customer_id__customer_third_hop_id__acquired_ds__month
                              , subq_18.customer_third_hop_id__acquired_ds__quarter AS customer_id__customer_third_hop_id__acquired_ds__quarter
                              , subq_18.customer_third_hop_id__acquired_ds__year AS customer_id__customer_third_hop_id__acquired_ds__year
                              , subq_18.customer_third_hop_id__acquired_ds__extract_year AS customer_id__customer_third_hop_id__acquired_ds__extract_year
                              , subq_18.customer_third_hop_id__acquired_ds__extract_quarter AS customer_id__customer_third_hop_id__acquired_ds__extract_quarter
                              , subq_18.customer_third_hop_id__acquired_ds__extract_month AS customer_id__customer_third_hop_id__acquired_ds__extract_month
                              , subq_18.customer_third_hop_id__acquired_ds__extract_day AS customer_id__customer_third_hop_id__acquired_ds__extract_day
                              , subq_18.customer_third_hop_id__acquired_ds__extract_dow AS customer_id__customer_third_hop_id__acquired_ds__extract_dow
                              , subq_18.customer_third_hop_id__acquired_ds__extract_doy AS customer_id__customer_third_hop_id__acquired_ds__extract_doy
                              , subq_18.metric_time__day AS customer_id__metric_time__day
                              , subq_18.metric_time__week AS customer_id__metric_time__week
                              , subq_18.metric_time__month AS customer_id__metric_time__month
                              , subq_18.metric_time__quarter AS customer_id__metric_time__quarter
                              , subq_18.metric_time__year AS customer_id__metric_time__year
                              , subq_18.metric_time__extract_year AS customer_id__metric_time__extract_year
                              , subq_18.metric_time__extract_quarter AS customer_id__metric_time__extract_quarter
                              , subq_18.metric_time__extract_month AS customer_id__metric_time__extract_month
                              , subq_18.metric_time__extract_day AS customer_id__metric_time__extract_day
                              , subq_18.metric_time__extract_dow AS customer_id__metric_time__extract_dow
                              , subq_18.metric_time__extract_doy AS customer_id__metric_time__extract_doy
                              , subq_18.customer_third_hop_id AS customer_id__customer_third_hop_id
                              , subq_18.customer_third_hop_id__customer_id AS customer_id__customer_third_hop_id__customer_id
                              , subq_15.ds_partitioned__day AS ds_partitioned__day
                              , subq_15.ds_partitioned__week AS ds_partitioned__week
                              , subq_15.ds_partitioned__month AS ds_partitioned__month
                              , subq_15.ds_partitioned__quarter AS ds_partitioned__quarter
                              , subq_15.ds_partitioned__year AS ds_partitioned__year
                              , subq_15.ds_partitioned__extract_year AS ds_partitioned__extract_year
                              , subq_15.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                              , subq_15.ds_partitioned__extract_month AS ds_partitioned__extract_month
                              , subq_15.ds_partitioned__extract_day AS ds_partitioned__extract_day
                              , subq_15.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                              , subq_15.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                              , subq_15.account_id__ds_partitioned__day AS account_id__ds_partitioned__day
                              , subq_15.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
                              , subq_15.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
                              , subq_15.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
                              , subq_15.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
                              , subq_15.account_id__ds_partitioned__extract_year AS account_id__ds_partitioned__extract_year
                              , subq_15.account_id__ds_partitioned__extract_quarter AS account_id__ds_partitioned__extract_quarter
                              , subq_15.account_id__ds_partitioned__extract_month AS account_id__ds_partitioned__extract_month
                              , subq_15.account_id__ds_partitioned__extract_day AS account_id__ds_partitioned__extract_day
                              , subq_15.account_id__ds_partitioned__extract_dow AS account_id__ds_partitioned__extract_dow
                              , subq_15.account_id__ds_partitioned__extract_doy AS account_id__ds_partitioned__extract_doy
                              , subq_15.bridge_account__ds_partitioned__day AS bridge_account__ds_partitioned__day
                              , subq_15.bridge_account__ds_partitioned__week AS bridge_account__ds_partitioned__week
                              , subq_15.bridge_account__ds_partitioned__month AS bridge_account__ds_partitioned__month
                              , subq_15.bridge_account__ds_partitioned__quarter AS bridge_account__ds_partitioned__quarter
                              , subq_15.bridge_account__ds_partitioned__year AS bridge_account__ds_partitioned__year
                              , subq_15.bridge_account__ds_partitioned__extract_year AS bridge_account__ds_partitioned__extract_year
                              , subq_15.bridge_account__ds_partitioned__extract_quarter AS bridge_account__ds_partitioned__extract_quarter
                              , subq_15.bridge_account__ds_partitioned__extract_month AS bridge_account__ds_partitioned__extract_month
                              , subq_15.bridge_account__ds_partitioned__extract_day AS bridge_account__ds_partitioned__extract_day
                              , subq_15.bridge_account__ds_partitioned__extract_dow AS bridge_account__ds_partitioned__extract_dow
                              , subq_15.bridge_account__ds_partitioned__extract_doy AS bridge_account__ds_partitioned__extract_doy
                              , subq_15.metric_time__day AS metric_time__day
                              , subq_15.metric_time__week AS metric_time__week
                              , subq_15.metric_time__month AS metric_time__month
                              , subq_15.metric_time__quarter AS metric_time__quarter
                              , subq_15.metric_time__year AS metric_time__year
                              , subq_15.metric_time__extract_year AS metric_time__extract_year
                              , subq_15.metric_time__extract_quarter AS metric_time__extract_quarter
                              , subq_15.metric_time__extract_month AS metric_time__extract_month
                              , subq_15.metric_time__extract_day AS metric_time__extract_day
                              , subq_15.metric_time__extract_dow AS metric_time__extract_dow
                              , subq_15.metric_time__extract_doy AS metric_time__extract_doy
                              , subq_15.account_id AS account_id
                              , subq_15.customer_id AS customer_id
                              , subq_15.account_id__customer_id AS account_id__customer_id
                              , subq_15.bridge_account__account_id AS bridge_account__account_id
                              , subq_15.bridge_account__customer_id AS bridge_account__customer_id
                              , subq_15.extra_dim AS extra_dim
                              , subq_15.account_id__extra_dim AS account_id__extra_dim
                              , subq_15.bridge_account__extra_dim AS bridge_account__extra_dim
                              , subq_15.__account_customer_combos AS __account_customer_combos
                            FROM (
                              -- Metric Time Dimension 'ds_partitioned'
                              SELECT
                                subq_14.ds_partitioned__day
                                , subq_14.ds_partitioned__week
                                , subq_14.ds_partitioned__month
                                , subq_14.ds_partitioned__quarter
                                , subq_14.ds_partitioned__year
                                , subq_14.ds_partitioned__extract_year
                                , subq_14.ds_partitioned__extract_quarter
                                , subq_14.ds_partitioned__extract_month
                                , subq_14.ds_partitioned__extract_day
                                , subq_14.ds_partitioned__extract_dow
                                , subq_14.ds_partitioned__extract_doy
                                , subq_14.account_id__ds_partitioned__day
                                , subq_14.account_id__ds_partitioned__week
                                , subq_14.account_id__ds_partitioned__month
                                , subq_14.account_id__ds_partitioned__quarter
                                , subq_14.account_id__ds_partitioned__year
                                , subq_14.account_id__ds_partitioned__extract_year
                                , subq_14.account_id__ds_partitioned__extract_quarter
                                , subq_14.account_id__ds_partitioned__extract_month
                                , subq_14.account_id__ds_partitioned__extract_day
                                , subq_14.account_id__ds_partitioned__extract_dow
                                , subq_14.account_id__ds_partitioned__extract_doy
                                , subq_14.bridge_account__ds_partitioned__day
                                , subq_14.bridge_account__ds_partitioned__week
                                , subq_14.bridge_account__ds_partitioned__month
                                , subq_14.bridge_account__ds_partitioned__quarter
                                , subq_14.bridge_account__ds_partitioned__year
                                , subq_14.bridge_account__ds_partitioned__extract_year
                                , subq_14.bridge_account__ds_partitioned__extract_quarter
                                , subq_14.bridge_account__ds_partitioned__extract_month
                                , subq_14.bridge_account__ds_partitioned__extract_day
                                , subq_14.bridge_account__ds_partitioned__extract_dow
                                , subq_14.bridge_account__ds_partitioned__extract_doy
                                , subq_14.ds_partitioned__day AS metric_time__day
                                , subq_14.ds_partitioned__week AS metric_time__week
                                , subq_14.ds_partitioned__month AS metric_time__month
                                , subq_14.ds_partitioned__quarter AS metric_time__quarter
                                , subq_14.ds_partitioned__year AS metric_time__year
                                , subq_14.ds_partitioned__extract_year AS metric_time__extract_year
                                , subq_14.ds_partitioned__extract_quarter AS metric_time__extract_quarter
                                , subq_14.ds_partitioned__extract_month AS metric_time__extract_month
                                , subq_14.ds_partitioned__extract_day AS metric_time__extract_day
                                , subq_14.ds_partitioned__extract_dow AS metric_time__extract_dow
                                , subq_14.ds_partitioned__extract_doy AS metric_time__extract_doy
                                , subq_14.account_id
                                , subq_14.customer_id
                                , subq_14.account_id__customer_id
                                , subq_14.bridge_account__account_id
                                , subq_14.bridge_account__customer_id
                                , subq_14.extra_dim
                                , subq_14.account_id__extra_dim
                                , subq_14.bridge_account__extra_dim
                                , subq_14.__account_customer_combos
                              FROM (
                                -- Read Elements From Semantic Model 'bridge_table'
                                SELECT
                                  account_id || customer_id AS __account_customer_combos
                                  , bridge_table_src_22000.extra_dim
                                  , DATE_TRUNC('day', bridge_table_src_22000.ds_partitioned) AS ds_partitioned__day
                                  , DATE_TRUNC('week', bridge_table_src_22000.ds_partitioned) AS ds_partitioned__week
                                  , DATE_TRUNC('month', bridge_table_src_22000.ds_partitioned) AS ds_partitioned__month
                                  , DATE_TRUNC('quarter', bridge_table_src_22000.ds_partitioned) AS ds_partitioned__quarter
                                  , DATE_TRUNC('year', bridge_table_src_22000.ds_partitioned) AS ds_partitioned__year
                                  , EXTRACT(year FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_year
                                  , EXTRACT(quarter FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_quarter
                                  , EXTRACT(month FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_month
                                  , EXTRACT(day FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_day
                                  , EXTRACT(DAY_OF_WEEK FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_dow
                                  , EXTRACT(doy FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_doy
                                  , bridge_table_src_22000.extra_dim AS account_id__extra_dim
                                  , DATE_TRUNC('day', bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__day
                                  , DATE_TRUNC('week', bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__week
                                  , DATE_TRUNC('month', bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__month
                                  , DATE_TRUNC('quarter', bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__quarter
                                  , DATE_TRUNC('year', bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__year
                                  , EXTRACT(year FROM bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_year
                                  , EXTRACT(quarter FROM bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_quarter
                                  , EXTRACT(month FROM bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_month
                                  , EXTRACT(day FROM bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_day
                                  , EXTRACT(DAY_OF_WEEK FROM bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_dow
                                  , EXTRACT(doy FROM bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_doy
                                  , bridge_table_src_22000.extra_dim AS bridge_account__extra_dim
                                  , DATE_TRUNC('day', bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__day
                                  , DATE_TRUNC('week', bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__week
                                  , DATE_TRUNC('month', bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__month
                                  , DATE_TRUNC('quarter', bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__quarter
                                  , DATE_TRUNC('year', bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__year
                                  , EXTRACT(year FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_year
                                  , EXTRACT(quarter FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_quarter
                                  , EXTRACT(month FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_month
                                  , EXTRACT(day FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_day
                                  , EXTRACT(DAY_OF_WEEK FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_dow
                                  , EXTRACT(doy FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_doy
                                  , bridge_table_src_22000.account_id
                                  , bridge_table_src_22000.customer_id
                                  , bridge_table_src_22000.customer_id AS account_id__customer_id
                                  , bridge_table_src_22000.account_id AS bridge_account__account_id
                                  , bridge_table_src_22000.customer_id AS bridge_account__customer_id
                                FROM ***************************.bridge_table bridge_table_src_22000
                              ) subq_14
                            ) subq_15
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
                                subq_17.acquired_ds__day
                                , subq_17.acquired_ds__week
                                , subq_17.acquired_ds__month
                                , subq_17.acquired_ds__quarter
                                , subq_17.acquired_ds__year
                                , subq_17.acquired_ds__extract_year
                                , subq_17.acquired_ds__extract_quarter
                                , subq_17.acquired_ds__extract_month
                                , subq_17.acquired_ds__extract_day
                                , subq_17.acquired_ds__extract_dow
                                , subq_17.acquired_ds__extract_doy
                                , subq_17.customer_id__acquired_ds__day
                                , subq_17.customer_id__acquired_ds__week
                                , subq_17.customer_id__acquired_ds__month
                                , subq_17.customer_id__acquired_ds__quarter
                                , subq_17.customer_id__acquired_ds__year
                                , subq_17.customer_id__acquired_ds__extract_year
                                , subq_17.customer_id__acquired_ds__extract_quarter
                                , subq_17.customer_id__acquired_ds__extract_month
                                , subq_17.customer_id__acquired_ds__extract_day
                                , subq_17.customer_id__acquired_ds__extract_dow
                                , subq_17.customer_id__acquired_ds__extract_doy
                                , subq_17.customer_third_hop_id__acquired_ds__day
                                , subq_17.customer_third_hop_id__acquired_ds__week
                                , subq_17.customer_third_hop_id__acquired_ds__month
                                , subq_17.customer_third_hop_id__acquired_ds__quarter
                                , subq_17.customer_third_hop_id__acquired_ds__year
                                , subq_17.customer_third_hop_id__acquired_ds__extract_year
                                , subq_17.customer_third_hop_id__acquired_ds__extract_quarter
                                , subq_17.customer_third_hop_id__acquired_ds__extract_month
                                , subq_17.customer_third_hop_id__acquired_ds__extract_day
                                , subq_17.customer_third_hop_id__acquired_ds__extract_dow
                                , subq_17.customer_third_hop_id__acquired_ds__extract_doy
                                , subq_17.metric_time__day
                                , subq_17.metric_time__week
                                , subq_17.metric_time__month
                                , subq_17.metric_time__quarter
                                , subq_17.metric_time__year
                                , subq_17.metric_time__extract_year
                                , subq_17.metric_time__extract_quarter
                                , subq_17.metric_time__extract_month
                                , subq_17.metric_time__extract_day
                                , subq_17.metric_time__extract_dow
                                , subq_17.metric_time__extract_doy
                                , subq_17.customer_id
                                , subq_17.customer_third_hop_id
                                , subq_17.customer_id__customer_third_hop_id
                                , subq_17.customer_third_hop_id__customer_id
                                , subq_17.country
                                , subq_17.customer_id__country
                                , subq_17.customer_third_hop_id__country
                              FROM (
                                -- Metric Time Dimension 'acquired_ds'
                                SELECT
                                  subq_16.acquired_ds__day
                                  , subq_16.acquired_ds__week
                                  , subq_16.acquired_ds__month
                                  , subq_16.acquired_ds__quarter
                                  , subq_16.acquired_ds__year
                                  , subq_16.acquired_ds__extract_year
                                  , subq_16.acquired_ds__extract_quarter
                                  , subq_16.acquired_ds__extract_month
                                  , subq_16.acquired_ds__extract_day
                                  , subq_16.acquired_ds__extract_dow
                                  , subq_16.acquired_ds__extract_doy
                                  , subq_16.customer_id__acquired_ds__day
                                  , subq_16.customer_id__acquired_ds__week
                                  , subq_16.customer_id__acquired_ds__month
                                  , subq_16.customer_id__acquired_ds__quarter
                                  , subq_16.customer_id__acquired_ds__year
                                  , subq_16.customer_id__acquired_ds__extract_year
                                  , subq_16.customer_id__acquired_ds__extract_quarter
                                  , subq_16.customer_id__acquired_ds__extract_month
                                  , subq_16.customer_id__acquired_ds__extract_day
                                  , subq_16.customer_id__acquired_ds__extract_dow
                                  , subq_16.customer_id__acquired_ds__extract_doy
                                  , subq_16.customer_third_hop_id__acquired_ds__day
                                  , subq_16.customer_third_hop_id__acquired_ds__week
                                  , subq_16.customer_third_hop_id__acquired_ds__month
                                  , subq_16.customer_third_hop_id__acquired_ds__quarter
                                  , subq_16.customer_third_hop_id__acquired_ds__year
                                  , subq_16.customer_third_hop_id__acquired_ds__extract_year
                                  , subq_16.customer_third_hop_id__acquired_ds__extract_quarter
                                  , subq_16.customer_third_hop_id__acquired_ds__extract_month
                                  , subq_16.customer_third_hop_id__acquired_ds__extract_day
                                  , subq_16.customer_third_hop_id__acquired_ds__extract_dow
                                  , subq_16.customer_third_hop_id__acquired_ds__extract_doy
                                  , subq_16.acquired_ds__day AS metric_time__day
                                  , subq_16.acquired_ds__week AS metric_time__week
                                  , subq_16.acquired_ds__month AS metric_time__month
                                  , subq_16.acquired_ds__quarter AS metric_time__quarter
                                  , subq_16.acquired_ds__year AS metric_time__year
                                  , subq_16.acquired_ds__extract_year AS metric_time__extract_year
                                  , subq_16.acquired_ds__extract_quarter AS metric_time__extract_quarter
                                  , subq_16.acquired_ds__extract_month AS metric_time__extract_month
                                  , subq_16.acquired_ds__extract_day AS metric_time__extract_day
                                  , subq_16.acquired_ds__extract_dow AS metric_time__extract_dow
                                  , subq_16.acquired_ds__extract_doy AS metric_time__extract_doy
                                  , subq_16.customer_id
                                  , subq_16.customer_third_hop_id
                                  , subq_16.customer_id__customer_third_hop_id
                                  , subq_16.customer_third_hop_id__customer_id
                                  , subq_16.country
                                  , subq_16.customer_id__country
                                  , subq_16.customer_third_hop_id__country
                                  , subq_16.__paraguayan_customers
                                FROM (
                                  -- Read Elements From Semantic Model 'customer_other_data'
                                  SELECT
                                    1 AS __paraguayan_customers
                                    , customer_other_data_src_22000.country
                                    , DATE_TRUNC('day', customer_other_data_src_22000.acquired_ds) AS acquired_ds__day
                                    , DATE_TRUNC('week', customer_other_data_src_22000.acquired_ds) AS acquired_ds__week
                                    , DATE_TRUNC('month', customer_other_data_src_22000.acquired_ds) AS acquired_ds__month
                                    , DATE_TRUNC('quarter', customer_other_data_src_22000.acquired_ds) AS acquired_ds__quarter
                                    , DATE_TRUNC('year', customer_other_data_src_22000.acquired_ds) AS acquired_ds__year
                                    , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_year
                                    , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_quarter
                                    , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_month
                                    , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_day
                                    , EXTRACT(DAY_OF_WEEK FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_dow
                                    , EXTRACT(doy FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_doy
                                    , customer_other_data_src_22000.country AS customer_id__country
                                    , DATE_TRUNC('day', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__day
                                    , DATE_TRUNC('week', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__week
                                    , DATE_TRUNC('month', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__month
                                    , DATE_TRUNC('quarter', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__quarter
                                    , DATE_TRUNC('year', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__year
                                    , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_year
                                    , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_quarter
                                    , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_month
                                    , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_day
                                    , EXTRACT(DAY_OF_WEEK FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_dow
                                    , EXTRACT(doy FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_doy
                                    , customer_other_data_src_22000.country AS customer_third_hop_id__country
                                    , DATE_TRUNC('day', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__day
                                    , DATE_TRUNC('week', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__week
                                    , DATE_TRUNC('month', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__month
                                    , DATE_TRUNC('quarter', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__quarter
                                    , DATE_TRUNC('year', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__year
                                    , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_year
                                    , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_quarter
                                    , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_month
                                    , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_day
                                    , EXTRACT(DAY_OF_WEEK FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_dow
                                    , EXTRACT(doy FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_doy
                                    , customer_other_data_src_22000.customer_id
                                    , customer_other_data_src_22000.customer_third_hop_id
                                    , customer_other_data_src_22000.customer_third_hop_id AS customer_id__customer_third_hop_id
                                    , customer_other_data_src_22000.customer_id AS customer_third_hop_id__customer_id
                                  FROM ***************************.customer_other_data customer_other_data_src_22000
                                ) subq_16
                              ) subq_17
                            ) subq_18
                            ON
                              subq_15.customer_id = subq_18.customer_id
                          ) subq_19
                        ) subq_20
                        ON
                          (
                            subq_13.account_id = subq_20.account_id
                          ) AND (
                            subq_13.ds_partitioned__day = subq_20.ds_partitioned__day
                          )
                      ) subq_21
                    ) subq_22
                  ) subq_23
                  GROUP BY
                    subq_23.account_id__customer_id__customer_third_hop_id
                ) subq_24
              ) subq_25
            ) subq_26
            ON
              subq_11.customer_third_hop_id = subq_26.account_id__customer_id__customer_third_hop_id
          ) subq_27
        ) subq_28
        WHERE customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count > 2
      ) subq_29
    ) subq_30
  ) subq_31
) subq_32
