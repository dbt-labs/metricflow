test_name: test_inner_query_multi_hop
test_filename: test_metric_filter_rendering.py
docstring:
  Tests rendering for a metric filter using a two-hop join in the inner query.
sql_engine: ClickHouse
---
SELECT
  subq_32.third_hop_count
FROM (
  SELECT
    subq_31.__third_hop_count AS third_hop_count
  FROM (
    SELECT
      COUNT(DISTINCT subq_30.__third_hop_count) AS __third_hop_count
    FROM (
      SELECT
        subq_29.__third_hop_count
      FROM (
        SELECT
          subq_28.third_hop_count AS __third_hop_count
          , subq_28.customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count
        FROM (
          SELECT
            subq_27.customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count
            , subq_27.__third_hop_count AS third_hop_count
          FROM (
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
                SELECT
                  third_hop_table_src_22000.customer_third_hop_id AS __third_hop_count
                  , third_hop_table_src_22000.value
                  , toStartOfDay(third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__day
                  , toStartOfWeek(third_hop_table_src_22000.third_hop_ds, 1) AS third_hop_ds__week
                  , toStartOfMonth(third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__month
                  , toStartOfQuarter(third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__quarter
                  , toStartOfYear(third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__year
                  , toYear(third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_year
                  , toQuarter(third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_quarter
                  , toMonth(third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_month
                  , toDayOfMonth(third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_day
                  , toDayOfWeek(third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_dow
                  , toDayOfYear(third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_doy
                  , third_hop_table_src_22000.value AS customer_third_hop_id__value
                  , toStartOfDay(third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__day
                  , toStartOfWeek(third_hop_table_src_22000.third_hop_ds, 1) AS customer_third_hop_id__third_hop_ds__week
                  , toStartOfMonth(third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__month
                  , toStartOfQuarter(third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__quarter
                  , toStartOfYear(third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__year
                  , toYear(third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_year
                  , toQuarter(third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_quarter
                  , toMonth(third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_month
                  , toDayOfMonth(third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_day
                  , toDayOfWeek(third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_dow
                  , toDayOfYear(third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_doy
                  , third_hop_table_src_22000.customer_third_hop_id
                FROM ***************************.third_hop_table third_hop_table_src_22000
              ) subq_10
            ) subq_11
            LEFT OUTER JOIN (
              SELECT
                subq_25.account_id__customer_id__customer_third_hop_id
                , subq_25.account_id__customer_id__customer_third_hop_id__txn_count
              FROM (
                SELECT
                  subq_24.account_id__customer_id__customer_third_hop_id
                  , subq_24.__txn_count AS account_id__customer_id__customer_third_hop_id__txn_count
                FROM (
                  SELECT
                    subq_23.account_id__customer_id__customer_third_hop_id
                    , SUM(subq_23.__txn_count) AS __txn_count
                  FROM (
                    SELECT
                      subq_22.account_id__customer_id__customer_third_hop_id
                      , subq_22.__txn_count
                    FROM (
                      SELECT
                        subq_21.account_id__customer_id__customer_third_hop_id
                        , subq_21.__txn_count
                      FROM (
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
                            SELECT
                              account_month_txns_src_22000.txn_count AS __txn_count
                              , toStartOfDay(account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__day
                              , toStartOfWeek(account_month_txns_src_22000.ds_partitioned, 1) AS ds_partitioned__week
                              , toStartOfMonth(account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__month
                              , toStartOfQuarter(account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__quarter
                              , toStartOfYear(account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__year
                              , toYear(account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_year
                              , toQuarter(account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_quarter
                              , toMonth(account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_month
                              , toDayOfMonth(account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_day
                              , toDayOfWeek(account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_dow
                              , toDayOfYear(account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_doy
                              , toStartOfDay(account_month_txns_src_22000.ds) AS ds__day
                              , toStartOfWeek(account_month_txns_src_22000.ds, 1) AS ds__week
                              , toStartOfMonth(account_month_txns_src_22000.ds) AS ds__month
                              , toStartOfQuarter(account_month_txns_src_22000.ds) AS ds__quarter
                              , toStartOfYear(account_month_txns_src_22000.ds) AS ds__year
                              , toYear(account_month_txns_src_22000.ds) AS ds__extract_year
                              , toQuarter(account_month_txns_src_22000.ds) AS ds__extract_quarter
                              , toMonth(account_month_txns_src_22000.ds) AS ds__extract_month
                              , toDayOfMonth(account_month_txns_src_22000.ds) AS ds__extract_day
                              , toDayOfWeek(account_month_txns_src_22000.ds) AS ds__extract_dow
                              , toDayOfYear(account_month_txns_src_22000.ds) AS ds__extract_doy
                              , account_month_txns_src_22000.account_month
                              , toStartOfDay(account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__day
                              , toStartOfWeek(account_month_txns_src_22000.ds_partitioned, 1) AS account_id__ds_partitioned__week
                              , toStartOfMonth(account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__month
                              , toStartOfQuarter(account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__quarter
                              , toStartOfYear(account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__year
                              , toYear(account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_year
                              , toQuarter(account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_quarter
                              , toMonth(account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_month
                              , toDayOfMonth(account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_day
                              , toDayOfWeek(account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_dow
                              , toDayOfYear(account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_doy
                              , toStartOfDay(account_month_txns_src_22000.ds) AS account_id__ds__day
                              , toStartOfWeek(account_month_txns_src_22000.ds, 1) AS account_id__ds__week
                              , toStartOfMonth(account_month_txns_src_22000.ds) AS account_id__ds__month
                              , toStartOfQuarter(account_month_txns_src_22000.ds) AS account_id__ds__quarter
                              , toStartOfYear(account_month_txns_src_22000.ds) AS account_id__ds__year
                              , toYear(account_month_txns_src_22000.ds) AS account_id__ds__extract_year
                              , toQuarter(account_month_txns_src_22000.ds) AS account_id__ds__extract_quarter
                              , toMonth(account_month_txns_src_22000.ds) AS account_id__ds__extract_month
                              , toDayOfMonth(account_month_txns_src_22000.ds) AS account_id__ds__extract_day
                              , toDayOfWeek(account_month_txns_src_22000.ds) AS account_id__ds__extract_dow
                              , toDayOfYear(account_month_txns_src_22000.ds) AS account_id__ds__extract_doy
                              , account_month_txns_src_22000.account_month AS account_id__account_month
                              , account_month_txns_src_22000.account_id
                            FROM ***************************.account_month_txns account_month_txns_src_22000
                          ) subq_12
                        ) subq_13
                        LEFT OUTER JOIN (
                          SELECT
                            subq_19.ds_partitioned__day
                            , subq_19.account_id
                            , subq_19.customer_id__customer_third_hop_id
                          FROM (
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
                                SELECT
                                  account_id || customer_id AS __account_customer_combos
                                  , bridge_table_src_22000.extra_dim
                                  , toStartOfDay(bridge_table_src_22000.ds_partitioned) AS ds_partitioned__day
                                  , toStartOfWeek(bridge_table_src_22000.ds_partitioned, 1) AS ds_partitioned__week
                                  , toStartOfMonth(bridge_table_src_22000.ds_partitioned) AS ds_partitioned__month
                                  , toStartOfQuarter(bridge_table_src_22000.ds_partitioned) AS ds_partitioned__quarter
                                  , toStartOfYear(bridge_table_src_22000.ds_partitioned) AS ds_partitioned__year
                                  , toYear(bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_year
                                  , toQuarter(bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_quarter
                                  , toMonth(bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_month
                                  , toDayOfMonth(bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_day
                                  , toDayOfWeek(bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_dow
                                  , toDayOfYear(bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_doy
                                  , bridge_table_src_22000.extra_dim AS account_id__extra_dim
                                  , toStartOfDay(bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__day
                                  , toStartOfWeek(bridge_table_src_22000.ds_partitioned, 1) AS account_id__ds_partitioned__week
                                  , toStartOfMonth(bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__month
                                  , toStartOfQuarter(bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__quarter
                                  , toStartOfYear(bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__year
                                  , toYear(bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_year
                                  , toQuarter(bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_quarter
                                  , toMonth(bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_month
                                  , toDayOfMonth(bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_day
                                  , toDayOfWeek(bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_dow
                                  , toDayOfYear(bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_doy
                                  , bridge_table_src_22000.extra_dim AS bridge_account__extra_dim
                                  , toStartOfDay(bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__day
                                  , toStartOfWeek(bridge_table_src_22000.ds_partitioned, 1) AS bridge_account__ds_partitioned__week
                                  , toStartOfMonth(bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__month
                                  , toStartOfQuarter(bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__quarter
                                  , toStartOfYear(bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__year
                                  , toYear(bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_year
                                  , toQuarter(bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_quarter
                                  , toMonth(bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_month
                                  , toDayOfMonth(bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_day
                                  , toDayOfWeek(bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_dow
                                  , toDayOfYear(bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_doy
                                  , bridge_table_src_22000.account_id
                                  , bridge_table_src_22000.customer_id
                                  , bridge_table_src_22000.customer_id AS account_id__customer_id
                                  , bridge_table_src_22000.account_id AS bridge_account__account_id
                                  , bridge_table_src_22000.customer_id AS bridge_account__customer_id
                                FROM ***************************.bridge_table bridge_table_src_22000
                              ) subq_14
                            ) subq_15
                            LEFT OUTER JOIN (
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
                                  SELECT
                                    1 AS __paraguayan_customers
                                    , customer_other_data_src_22000.country
                                    , toStartOfDay(customer_other_data_src_22000.acquired_ds) AS acquired_ds__day
                                    , toStartOfWeek(customer_other_data_src_22000.acquired_ds, 1) AS acquired_ds__week
                                    , toStartOfMonth(customer_other_data_src_22000.acquired_ds) AS acquired_ds__month
                                    , toStartOfQuarter(customer_other_data_src_22000.acquired_ds) AS acquired_ds__quarter
                                    , toStartOfYear(customer_other_data_src_22000.acquired_ds) AS acquired_ds__year
                                    , toYear(customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_year
                                    , toQuarter(customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_quarter
                                    , toMonth(customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_month
                                    , toDayOfMonth(customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_day
                                    , toDayOfWeek(customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_dow
                                    , toDayOfYear(customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_doy
                                    , customer_other_data_src_22000.country AS customer_id__country
                                    , toStartOfDay(customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__day
                                    , toStartOfWeek(customer_other_data_src_22000.acquired_ds, 1) AS customer_id__acquired_ds__week
                                    , toStartOfMonth(customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__month
                                    , toStartOfQuarter(customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__quarter
                                    , toStartOfYear(customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__year
                                    , toYear(customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_year
                                    , toQuarter(customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_quarter
                                    , toMonth(customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_month
                                    , toDayOfMonth(customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_day
                                    , toDayOfWeek(customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_dow
                                    , toDayOfYear(customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_doy
                                    , customer_other_data_src_22000.country AS customer_third_hop_id__country
                                    , toStartOfDay(customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__day
                                    , toStartOfWeek(customer_other_data_src_22000.acquired_ds, 1) AS customer_third_hop_id__acquired_ds__week
                                    , toStartOfMonth(customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__month
                                    , toStartOfQuarter(customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__quarter
                                    , toStartOfYear(customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__year
                                    , toYear(customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_year
                                    , toQuarter(customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_quarter
                                    , toMonth(customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_month
                                    , toDayOfMonth(customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_day
                                    , toDayOfWeek(customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_dow
                                    , toDayOfYear(customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_doy
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
