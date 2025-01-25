test_name: test_inner_query_multi_hop
test_filename: test_metric_filter_rendering.py
docstring:
  Tests rendering for a metric filter using a two-hop join in the inner query.
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_24.third_hop_count
FROM (
  -- Aggregate Measures
  SELECT
    COUNT(DISTINCT nr_subq_23.third_hop_count) AS third_hop_count
  FROM (
    -- Pass Only Elements: ['third_hop_count',]
    SELECT
      nr_subq_22.third_hop_count
    FROM (
      -- Constrain Output with WHERE
      SELECT
        nr_subq_21.third_hop_ds__day
        , nr_subq_21.third_hop_ds__week
        , nr_subq_21.third_hop_ds__month
        , nr_subq_21.third_hop_ds__quarter
        , nr_subq_21.third_hop_ds__year
        , nr_subq_21.third_hop_ds__extract_year
        , nr_subq_21.third_hop_ds__extract_quarter
        , nr_subq_21.third_hop_ds__extract_month
        , nr_subq_21.third_hop_ds__extract_day
        , nr_subq_21.third_hop_ds__extract_dow
        , nr_subq_21.third_hop_ds__extract_doy
        , nr_subq_21.customer_third_hop_id__third_hop_ds__day
        , nr_subq_21.customer_third_hop_id__third_hop_ds__week
        , nr_subq_21.customer_third_hop_id__third_hop_ds__month
        , nr_subq_21.customer_third_hop_id__third_hop_ds__quarter
        , nr_subq_21.customer_third_hop_id__third_hop_ds__year
        , nr_subq_21.customer_third_hop_id__third_hop_ds__extract_year
        , nr_subq_21.customer_third_hop_id__third_hop_ds__extract_quarter
        , nr_subq_21.customer_third_hop_id__third_hop_ds__extract_month
        , nr_subq_21.customer_third_hop_id__third_hop_ds__extract_day
        , nr_subq_21.customer_third_hop_id__third_hop_ds__extract_dow
        , nr_subq_21.customer_third_hop_id__third_hop_ds__extract_doy
        , nr_subq_21.metric_time__day
        , nr_subq_21.metric_time__week
        , nr_subq_21.metric_time__month
        , nr_subq_21.metric_time__quarter
        , nr_subq_21.metric_time__year
        , nr_subq_21.metric_time__extract_year
        , nr_subq_21.metric_time__extract_quarter
        , nr_subq_21.metric_time__extract_month
        , nr_subq_21.metric_time__extract_day
        , nr_subq_21.metric_time__extract_dow
        , nr_subq_21.metric_time__extract_doy
        , nr_subq_21.customer_third_hop_id
        , nr_subq_21.customer_third_hop_id__account_id__customer_id__customer_third_hop_id
        , nr_subq_21.value
        , nr_subq_21.customer_third_hop_id__value
        , nr_subq_21.customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count
        , nr_subq_21.third_hop_count
      FROM (
        -- Join Standard Outputs
        SELECT
          nr_subq_20.account_id__customer_id__customer_third_hop_id AS customer_third_hop_id__account_id__customer_id__customer_third_hop_id
          , nr_subq_20.account_id__customer_id__customer_third_hop_id__txn_count AS customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count
          , nr_subq_9.third_hop_ds__day AS third_hop_ds__day
          , nr_subq_9.third_hop_ds__week AS third_hop_ds__week
          , nr_subq_9.third_hop_ds__month AS third_hop_ds__month
          , nr_subq_9.third_hop_ds__quarter AS third_hop_ds__quarter
          , nr_subq_9.third_hop_ds__year AS third_hop_ds__year
          , nr_subq_9.third_hop_ds__extract_year AS third_hop_ds__extract_year
          , nr_subq_9.third_hop_ds__extract_quarter AS third_hop_ds__extract_quarter
          , nr_subq_9.third_hop_ds__extract_month AS third_hop_ds__extract_month
          , nr_subq_9.third_hop_ds__extract_day AS third_hop_ds__extract_day
          , nr_subq_9.third_hop_ds__extract_dow AS third_hop_ds__extract_dow
          , nr_subq_9.third_hop_ds__extract_doy AS third_hop_ds__extract_doy
          , nr_subq_9.customer_third_hop_id__third_hop_ds__day AS customer_third_hop_id__third_hop_ds__day
          , nr_subq_9.customer_third_hop_id__third_hop_ds__week AS customer_third_hop_id__third_hop_ds__week
          , nr_subq_9.customer_third_hop_id__third_hop_ds__month AS customer_third_hop_id__third_hop_ds__month
          , nr_subq_9.customer_third_hop_id__third_hop_ds__quarter AS customer_third_hop_id__third_hop_ds__quarter
          , nr_subq_9.customer_third_hop_id__third_hop_ds__year AS customer_third_hop_id__third_hop_ds__year
          , nr_subq_9.customer_third_hop_id__third_hop_ds__extract_year AS customer_third_hop_id__third_hop_ds__extract_year
          , nr_subq_9.customer_third_hop_id__third_hop_ds__extract_quarter AS customer_third_hop_id__third_hop_ds__extract_quarter
          , nr_subq_9.customer_third_hop_id__third_hop_ds__extract_month AS customer_third_hop_id__third_hop_ds__extract_month
          , nr_subq_9.customer_third_hop_id__third_hop_ds__extract_day AS customer_third_hop_id__third_hop_ds__extract_day
          , nr_subq_9.customer_third_hop_id__third_hop_ds__extract_dow AS customer_third_hop_id__third_hop_ds__extract_dow
          , nr_subq_9.customer_third_hop_id__third_hop_ds__extract_doy AS customer_third_hop_id__third_hop_ds__extract_doy
          , nr_subq_9.metric_time__day AS metric_time__day
          , nr_subq_9.metric_time__week AS metric_time__week
          , nr_subq_9.metric_time__month AS metric_time__month
          , nr_subq_9.metric_time__quarter AS metric_time__quarter
          , nr_subq_9.metric_time__year AS metric_time__year
          , nr_subq_9.metric_time__extract_year AS metric_time__extract_year
          , nr_subq_9.metric_time__extract_quarter AS metric_time__extract_quarter
          , nr_subq_9.metric_time__extract_month AS metric_time__extract_month
          , nr_subq_9.metric_time__extract_day AS metric_time__extract_day
          , nr_subq_9.metric_time__extract_dow AS metric_time__extract_dow
          , nr_subq_9.metric_time__extract_doy AS metric_time__extract_doy
          , nr_subq_9.customer_third_hop_id AS customer_third_hop_id
          , nr_subq_9.value AS value
          , nr_subq_9.customer_third_hop_id__value AS customer_third_hop_id__value
          , nr_subq_9.third_hop_count AS third_hop_count
        FROM (
          -- Metric Time Dimension 'third_hop_ds'
          SELECT
            nr_subq_22004.third_hop_ds__day
            , nr_subq_22004.third_hop_ds__week
            , nr_subq_22004.third_hop_ds__month
            , nr_subq_22004.third_hop_ds__quarter
            , nr_subq_22004.third_hop_ds__year
            , nr_subq_22004.third_hop_ds__extract_year
            , nr_subq_22004.third_hop_ds__extract_quarter
            , nr_subq_22004.third_hop_ds__extract_month
            , nr_subq_22004.third_hop_ds__extract_day
            , nr_subq_22004.third_hop_ds__extract_dow
            , nr_subq_22004.third_hop_ds__extract_doy
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__day
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__week
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__month
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__quarter
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__year
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__extract_year
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__extract_quarter
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__extract_month
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__extract_day
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__extract_dow
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__extract_doy
            , nr_subq_22004.third_hop_ds__day AS metric_time__day
            , nr_subq_22004.third_hop_ds__week AS metric_time__week
            , nr_subq_22004.third_hop_ds__month AS metric_time__month
            , nr_subq_22004.third_hop_ds__quarter AS metric_time__quarter
            , nr_subq_22004.third_hop_ds__year AS metric_time__year
            , nr_subq_22004.third_hop_ds__extract_year AS metric_time__extract_year
            , nr_subq_22004.third_hop_ds__extract_quarter AS metric_time__extract_quarter
            , nr_subq_22004.third_hop_ds__extract_month AS metric_time__extract_month
            , nr_subq_22004.third_hop_ds__extract_day AS metric_time__extract_day
            , nr_subq_22004.third_hop_ds__extract_dow AS metric_time__extract_dow
            , nr_subq_22004.third_hop_ds__extract_doy AS metric_time__extract_doy
            , nr_subq_22004.customer_third_hop_id
            , nr_subq_22004.value
            , nr_subq_22004.customer_third_hop_id__value
            , nr_subq_22004.third_hop_count
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
          ) nr_subq_22004
        ) nr_subq_9
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['account_id__customer_id__customer_third_hop_id', 'account_id__customer_id__customer_third_hop_id__txn_count']
          SELECT
            nr_subq_19.account_id__customer_id__customer_third_hop_id
            , nr_subq_19.account_id__customer_id__customer_third_hop_id__txn_count
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              nr_subq_18.account_id__customer_id__customer_third_hop_id
              , nr_subq_18.txn_count AS account_id__customer_id__customer_third_hop_id__txn_count
            FROM (
              -- Aggregate Measures
              SELECT
                nr_subq_17.account_id__customer_id__customer_third_hop_id
                , SUM(nr_subq_17.txn_count) AS txn_count
              FROM (
                -- Pass Only Elements: ['txn_count', 'account_id__customer_id__customer_third_hop_id']
                SELECT
                  nr_subq_16.account_id__customer_id__customer_third_hop_id
                  , nr_subq_16.txn_count
                FROM (
                  -- Join Standard Outputs
                  SELECT
                    nr_subq_15.ds_partitioned__day AS account_id__ds_partitioned__day
                    , nr_subq_15.customer_id__customer_third_hop_id AS account_id__customer_id__customer_third_hop_id
                    , nr_subq_10.ds_partitioned__day AS ds_partitioned__day
                    , nr_subq_10.ds_partitioned__week AS ds_partitioned__week
                    , nr_subq_10.ds_partitioned__month AS ds_partitioned__month
                    , nr_subq_10.ds_partitioned__quarter AS ds_partitioned__quarter
                    , nr_subq_10.ds_partitioned__year AS ds_partitioned__year
                    , nr_subq_10.ds_partitioned__extract_year AS ds_partitioned__extract_year
                    , nr_subq_10.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                    , nr_subq_10.ds_partitioned__extract_month AS ds_partitioned__extract_month
                    , nr_subq_10.ds_partitioned__extract_day AS ds_partitioned__extract_day
                    , nr_subq_10.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                    , nr_subq_10.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                    , nr_subq_10.ds__day AS ds__day
                    , nr_subq_10.ds__week AS ds__week
                    , nr_subq_10.ds__month AS ds__month
                    , nr_subq_10.ds__quarter AS ds__quarter
                    , nr_subq_10.ds__year AS ds__year
                    , nr_subq_10.ds__extract_year AS ds__extract_year
                    , nr_subq_10.ds__extract_quarter AS ds__extract_quarter
                    , nr_subq_10.ds__extract_month AS ds__extract_month
                    , nr_subq_10.ds__extract_day AS ds__extract_day
                    , nr_subq_10.ds__extract_dow AS ds__extract_dow
                    , nr_subq_10.ds__extract_doy AS ds__extract_doy
                    , nr_subq_10.account_id__ds_partitioned__day AS account_id__ds_partitioned__day
                    , nr_subq_10.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
                    , nr_subq_10.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
                    , nr_subq_10.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
                    , nr_subq_10.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
                    , nr_subq_10.account_id__ds_partitioned__extract_year AS account_id__ds_partitioned__extract_year
                    , nr_subq_10.account_id__ds_partitioned__extract_quarter AS account_id__ds_partitioned__extract_quarter
                    , nr_subq_10.account_id__ds_partitioned__extract_month AS account_id__ds_partitioned__extract_month
                    , nr_subq_10.account_id__ds_partitioned__extract_day AS account_id__ds_partitioned__extract_day
                    , nr_subq_10.account_id__ds_partitioned__extract_dow AS account_id__ds_partitioned__extract_dow
                    , nr_subq_10.account_id__ds_partitioned__extract_doy AS account_id__ds_partitioned__extract_doy
                    , nr_subq_10.account_id__ds__day AS account_id__ds__day
                    , nr_subq_10.account_id__ds__week AS account_id__ds__week
                    , nr_subq_10.account_id__ds__month AS account_id__ds__month
                    , nr_subq_10.account_id__ds__quarter AS account_id__ds__quarter
                    , nr_subq_10.account_id__ds__year AS account_id__ds__year
                    , nr_subq_10.account_id__ds__extract_year AS account_id__ds__extract_year
                    , nr_subq_10.account_id__ds__extract_quarter AS account_id__ds__extract_quarter
                    , nr_subq_10.account_id__ds__extract_month AS account_id__ds__extract_month
                    , nr_subq_10.account_id__ds__extract_day AS account_id__ds__extract_day
                    , nr_subq_10.account_id__ds__extract_dow AS account_id__ds__extract_dow
                    , nr_subq_10.account_id__ds__extract_doy AS account_id__ds__extract_doy
                    , nr_subq_10.metric_time__day AS metric_time__day
                    , nr_subq_10.metric_time__week AS metric_time__week
                    , nr_subq_10.metric_time__month AS metric_time__month
                    , nr_subq_10.metric_time__quarter AS metric_time__quarter
                    , nr_subq_10.metric_time__year AS metric_time__year
                    , nr_subq_10.metric_time__extract_year AS metric_time__extract_year
                    , nr_subq_10.metric_time__extract_quarter AS metric_time__extract_quarter
                    , nr_subq_10.metric_time__extract_month AS metric_time__extract_month
                    , nr_subq_10.metric_time__extract_day AS metric_time__extract_day
                    , nr_subq_10.metric_time__extract_dow AS metric_time__extract_dow
                    , nr_subq_10.metric_time__extract_doy AS metric_time__extract_doy
                    , nr_subq_10.account_id AS account_id
                    , nr_subq_10.account_month AS account_month
                    , nr_subq_10.account_id__account_month AS account_id__account_month
                    , nr_subq_10.txn_count AS txn_count
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      nr_subq_22000.ds_partitioned__day
                      , nr_subq_22000.ds_partitioned__week
                      , nr_subq_22000.ds_partitioned__month
                      , nr_subq_22000.ds_partitioned__quarter
                      , nr_subq_22000.ds_partitioned__year
                      , nr_subq_22000.ds_partitioned__extract_year
                      , nr_subq_22000.ds_partitioned__extract_quarter
                      , nr_subq_22000.ds_partitioned__extract_month
                      , nr_subq_22000.ds_partitioned__extract_day
                      , nr_subq_22000.ds_partitioned__extract_dow
                      , nr_subq_22000.ds_partitioned__extract_doy
                      , nr_subq_22000.ds__day
                      , nr_subq_22000.ds__week
                      , nr_subq_22000.ds__month
                      , nr_subq_22000.ds__quarter
                      , nr_subq_22000.ds__year
                      , nr_subq_22000.ds__extract_year
                      , nr_subq_22000.ds__extract_quarter
                      , nr_subq_22000.ds__extract_month
                      , nr_subq_22000.ds__extract_day
                      , nr_subq_22000.ds__extract_dow
                      , nr_subq_22000.ds__extract_doy
                      , nr_subq_22000.account_id__ds_partitioned__day
                      , nr_subq_22000.account_id__ds_partitioned__week
                      , nr_subq_22000.account_id__ds_partitioned__month
                      , nr_subq_22000.account_id__ds_partitioned__quarter
                      , nr_subq_22000.account_id__ds_partitioned__year
                      , nr_subq_22000.account_id__ds_partitioned__extract_year
                      , nr_subq_22000.account_id__ds_partitioned__extract_quarter
                      , nr_subq_22000.account_id__ds_partitioned__extract_month
                      , nr_subq_22000.account_id__ds_partitioned__extract_day
                      , nr_subq_22000.account_id__ds_partitioned__extract_dow
                      , nr_subq_22000.account_id__ds_partitioned__extract_doy
                      , nr_subq_22000.account_id__ds__day
                      , nr_subq_22000.account_id__ds__week
                      , nr_subq_22000.account_id__ds__month
                      , nr_subq_22000.account_id__ds__quarter
                      , nr_subq_22000.account_id__ds__year
                      , nr_subq_22000.account_id__ds__extract_year
                      , nr_subq_22000.account_id__ds__extract_quarter
                      , nr_subq_22000.account_id__ds__extract_month
                      , nr_subq_22000.account_id__ds__extract_day
                      , nr_subq_22000.account_id__ds__extract_dow
                      , nr_subq_22000.account_id__ds__extract_doy
                      , nr_subq_22000.ds__day AS metric_time__day
                      , nr_subq_22000.ds__week AS metric_time__week
                      , nr_subq_22000.ds__month AS metric_time__month
                      , nr_subq_22000.ds__quarter AS metric_time__quarter
                      , nr_subq_22000.ds__year AS metric_time__year
                      , nr_subq_22000.ds__extract_year AS metric_time__extract_year
                      , nr_subq_22000.ds__extract_quarter AS metric_time__extract_quarter
                      , nr_subq_22000.ds__extract_month AS metric_time__extract_month
                      , nr_subq_22000.ds__extract_day AS metric_time__extract_day
                      , nr_subq_22000.ds__extract_dow AS metric_time__extract_dow
                      , nr_subq_22000.ds__extract_doy AS metric_time__extract_doy
                      , nr_subq_22000.account_id
                      , nr_subq_22000.account_month
                      , nr_subq_22000.account_id__account_month
                      , nr_subq_22000.txn_count
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
                    ) nr_subq_22000
                  ) nr_subq_10
                  LEFT OUTER JOIN (
                    -- Pass Only Elements: ['ds_partitioned__day', 'account_id', 'customer_id__customer_third_hop_id']
                    SELECT
                      nr_subq_14.ds_partitioned__day
                      , nr_subq_14.account_id
                      , nr_subq_14.customer_id__customer_third_hop_id
                    FROM (
                      -- Join Standard Outputs
                      SELECT
                        nr_subq_13.country AS customer_id__country
                        , nr_subq_13.customer_third_hop_id__country AS customer_id__customer_third_hop_id__country
                        , nr_subq_13.acquired_ds__day AS customer_id__acquired_ds__day
                        , nr_subq_13.acquired_ds__week AS customer_id__acquired_ds__week
                        , nr_subq_13.acquired_ds__month AS customer_id__acquired_ds__month
                        , nr_subq_13.acquired_ds__quarter AS customer_id__acquired_ds__quarter
                        , nr_subq_13.acquired_ds__year AS customer_id__acquired_ds__year
                        , nr_subq_13.acquired_ds__extract_year AS customer_id__acquired_ds__extract_year
                        , nr_subq_13.acquired_ds__extract_quarter AS customer_id__acquired_ds__extract_quarter
                        , nr_subq_13.acquired_ds__extract_month AS customer_id__acquired_ds__extract_month
                        , nr_subq_13.acquired_ds__extract_day AS customer_id__acquired_ds__extract_day
                        , nr_subq_13.acquired_ds__extract_dow AS customer_id__acquired_ds__extract_dow
                        , nr_subq_13.acquired_ds__extract_doy AS customer_id__acquired_ds__extract_doy
                        , nr_subq_13.customer_third_hop_id__acquired_ds__day AS customer_id__customer_third_hop_id__acquired_ds__day
                        , nr_subq_13.customer_third_hop_id__acquired_ds__week AS customer_id__customer_third_hop_id__acquired_ds__week
                        , nr_subq_13.customer_third_hop_id__acquired_ds__month AS customer_id__customer_third_hop_id__acquired_ds__month
                        , nr_subq_13.customer_third_hop_id__acquired_ds__quarter AS customer_id__customer_third_hop_id__acquired_ds__quarter
                        , nr_subq_13.customer_third_hop_id__acquired_ds__year AS customer_id__customer_third_hop_id__acquired_ds__year
                        , nr_subq_13.customer_third_hop_id__acquired_ds__extract_year AS customer_id__customer_third_hop_id__acquired_ds__extract_year
                        , nr_subq_13.customer_third_hop_id__acquired_ds__extract_quarter AS customer_id__customer_third_hop_id__acquired_ds__extract_quarter
                        , nr_subq_13.customer_third_hop_id__acquired_ds__extract_month AS customer_id__customer_third_hop_id__acquired_ds__extract_month
                        , nr_subq_13.customer_third_hop_id__acquired_ds__extract_day AS customer_id__customer_third_hop_id__acquired_ds__extract_day
                        , nr_subq_13.customer_third_hop_id__acquired_ds__extract_dow AS customer_id__customer_third_hop_id__acquired_ds__extract_dow
                        , nr_subq_13.customer_third_hop_id__acquired_ds__extract_doy AS customer_id__customer_third_hop_id__acquired_ds__extract_doy
                        , nr_subq_13.metric_time__day AS customer_id__metric_time__day
                        , nr_subq_13.metric_time__week AS customer_id__metric_time__week
                        , nr_subq_13.metric_time__month AS customer_id__metric_time__month
                        , nr_subq_13.metric_time__quarter AS customer_id__metric_time__quarter
                        , nr_subq_13.metric_time__year AS customer_id__metric_time__year
                        , nr_subq_13.metric_time__extract_year AS customer_id__metric_time__extract_year
                        , nr_subq_13.metric_time__extract_quarter AS customer_id__metric_time__extract_quarter
                        , nr_subq_13.metric_time__extract_month AS customer_id__metric_time__extract_month
                        , nr_subq_13.metric_time__extract_day AS customer_id__metric_time__extract_day
                        , nr_subq_13.metric_time__extract_dow AS customer_id__metric_time__extract_dow
                        , nr_subq_13.metric_time__extract_doy AS customer_id__metric_time__extract_doy
                        , nr_subq_13.customer_third_hop_id AS customer_id__customer_third_hop_id
                        , nr_subq_13.customer_third_hop_id__customer_id AS customer_id__customer_third_hop_id__customer_id
                        , nr_subq_11.ds_partitioned__day AS ds_partitioned__day
                        , nr_subq_11.ds_partitioned__week AS ds_partitioned__week
                        , nr_subq_11.ds_partitioned__month AS ds_partitioned__month
                        , nr_subq_11.ds_partitioned__quarter AS ds_partitioned__quarter
                        , nr_subq_11.ds_partitioned__year AS ds_partitioned__year
                        , nr_subq_11.ds_partitioned__extract_year AS ds_partitioned__extract_year
                        , nr_subq_11.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                        , nr_subq_11.ds_partitioned__extract_month AS ds_partitioned__extract_month
                        , nr_subq_11.ds_partitioned__extract_day AS ds_partitioned__extract_day
                        , nr_subq_11.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                        , nr_subq_11.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                        , nr_subq_11.account_id__ds_partitioned__day AS account_id__ds_partitioned__day
                        , nr_subq_11.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
                        , nr_subq_11.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
                        , nr_subq_11.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
                        , nr_subq_11.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
                        , nr_subq_11.account_id__ds_partitioned__extract_year AS account_id__ds_partitioned__extract_year
                        , nr_subq_11.account_id__ds_partitioned__extract_quarter AS account_id__ds_partitioned__extract_quarter
                        , nr_subq_11.account_id__ds_partitioned__extract_month AS account_id__ds_partitioned__extract_month
                        , nr_subq_11.account_id__ds_partitioned__extract_day AS account_id__ds_partitioned__extract_day
                        , nr_subq_11.account_id__ds_partitioned__extract_dow AS account_id__ds_partitioned__extract_dow
                        , nr_subq_11.account_id__ds_partitioned__extract_doy AS account_id__ds_partitioned__extract_doy
                        , nr_subq_11.bridge_account__ds_partitioned__day AS bridge_account__ds_partitioned__day
                        , nr_subq_11.bridge_account__ds_partitioned__week AS bridge_account__ds_partitioned__week
                        , nr_subq_11.bridge_account__ds_partitioned__month AS bridge_account__ds_partitioned__month
                        , nr_subq_11.bridge_account__ds_partitioned__quarter AS bridge_account__ds_partitioned__quarter
                        , nr_subq_11.bridge_account__ds_partitioned__year AS bridge_account__ds_partitioned__year
                        , nr_subq_11.bridge_account__ds_partitioned__extract_year AS bridge_account__ds_partitioned__extract_year
                        , nr_subq_11.bridge_account__ds_partitioned__extract_quarter AS bridge_account__ds_partitioned__extract_quarter
                        , nr_subq_11.bridge_account__ds_partitioned__extract_month AS bridge_account__ds_partitioned__extract_month
                        , nr_subq_11.bridge_account__ds_partitioned__extract_day AS bridge_account__ds_partitioned__extract_day
                        , nr_subq_11.bridge_account__ds_partitioned__extract_dow AS bridge_account__ds_partitioned__extract_dow
                        , nr_subq_11.bridge_account__ds_partitioned__extract_doy AS bridge_account__ds_partitioned__extract_doy
                        , nr_subq_11.metric_time__day AS metric_time__day
                        , nr_subq_11.metric_time__week AS metric_time__week
                        , nr_subq_11.metric_time__month AS metric_time__month
                        , nr_subq_11.metric_time__quarter AS metric_time__quarter
                        , nr_subq_11.metric_time__year AS metric_time__year
                        , nr_subq_11.metric_time__extract_year AS metric_time__extract_year
                        , nr_subq_11.metric_time__extract_quarter AS metric_time__extract_quarter
                        , nr_subq_11.metric_time__extract_month AS metric_time__extract_month
                        , nr_subq_11.metric_time__extract_day AS metric_time__extract_day
                        , nr_subq_11.metric_time__extract_dow AS metric_time__extract_dow
                        , nr_subq_11.metric_time__extract_doy AS metric_time__extract_doy
                        , nr_subq_11.account_id AS account_id
                        , nr_subq_11.customer_id AS customer_id
                        , nr_subq_11.account_id__customer_id AS account_id__customer_id
                        , nr_subq_11.bridge_account__account_id AS bridge_account__account_id
                        , nr_subq_11.bridge_account__customer_id AS bridge_account__customer_id
                        , nr_subq_11.extra_dim AS extra_dim
                        , nr_subq_11.account_id__extra_dim AS account_id__extra_dim
                        , nr_subq_11.bridge_account__extra_dim AS bridge_account__extra_dim
                        , nr_subq_11.account_customer_combos AS account_customer_combos
                      FROM (
                        -- Metric Time Dimension 'ds_partitioned'
                        SELECT
                          nr_subq_22001.ds_partitioned__day
                          , nr_subq_22001.ds_partitioned__week
                          , nr_subq_22001.ds_partitioned__month
                          , nr_subq_22001.ds_partitioned__quarter
                          , nr_subq_22001.ds_partitioned__year
                          , nr_subq_22001.ds_partitioned__extract_year
                          , nr_subq_22001.ds_partitioned__extract_quarter
                          , nr_subq_22001.ds_partitioned__extract_month
                          , nr_subq_22001.ds_partitioned__extract_day
                          , nr_subq_22001.ds_partitioned__extract_dow
                          , nr_subq_22001.ds_partitioned__extract_doy
                          , nr_subq_22001.account_id__ds_partitioned__day
                          , nr_subq_22001.account_id__ds_partitioned__week
                          , nr_subq_22001.account_id__ds_partitioned__month
                          , nr_subq_22001.account_id__ds_partitioned__quarter
                          , nr_subq_22001.account_id__ds_partitioned__year
                          , nr_subq_22001.account_id__ds_partitioned__extract_year
                          , nr_subq_22001.account_id__ds_partitioned__extract_quarter
                          , nr_subq_22001.account_id__ds_partitioned__extract_month
                          , nr_subq_22001.account_id__ds_partitioned__extract_day
                          , nr_subq_22001.account_id__ds_partitioned__extract_dow
                          , nr_subq_22001.account_id__ds_partitioned__extract_doy
                          , nr_subq_22001.bridge_account__ds_partitioned__day
                          , nr_subq_22001.bridge_account__ds_partitioned__week
                          , nr_subq_22001.bridge_account__ds_partitioned__month
                          , nr_subq_22001.bridge_account__ds_partitioned__quarter
                          , nr_subq_22001.bridge_account__ds_partitioned__year
                          , nr_subq_22001.bridge_account__ds_partitioned__extract_year
                          , nr_subq_22001.bridge_account__ds_partitioned__extract_quarter
                          , nr_subq_22001.bridge_account__ds_partitioned__extract_month
                          , nr_subq_22001.bridge_account__ds_partitioned__extract_day
                          , nr_subq_22001.bridge_account__ds_partitioned__extract_dow
                          , nr_subq_22001.bridge_account__ds_partitioned__extract_doy
                          , nr_subq_22001.ds_partitioned__day AS metric_time__day
                          , nr_subq_22001.ds_partitioned__week AS metric_time__week
                          , nr_subq_22001.ds_partitioned__month AS metric_time__month
                          , nr_subq_22001.ds_partitioned__quarter AS metric_time__quarter
                          , nr_subq_22001.ds_partitioned__year AS metric_time__year
                          , nr_subq_22001.ds_partitioned__extract_year AS metric_time__extract_year
                          , nr_subq_22001.ds_partitioned__extract_quarter AS metric_time__extract_quarter
                          , nr_subq_22001.ds_partitioned__extract_month AS metric_time__extract_month
                          , nr_subq_22001.ds_partitioned__extract_day AS metric_time__extract_day
                          , nr_subq_22001.ds_partitioned__extract_dow AS metric_time__extract_dow
                          , nr_subq_22001.ds_partitioned__extract_doy AS metric_time__extract_doy
                          , nr_subq_22001.account_id
                          , nr_subq_22001.customer_id
                          , nr_subq_22001.account_id__customer_id
                          , nr_subq_22001.bridge_account__account_id
                          , nr_subq_22001.bridge_account__customer_id
                          , nr_subq_22001.extra_dim
                          , nr_subq_22001.account_id__extra_dim
                          , nr_subq_22001.bridge_account__extra_dim
                          , nr_subq_22001.account_customer_combos
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
                        ) nr_subq_22001
                      ) nr_subq_11
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
                          nr_subq_12.acquired_ds__day
                          , nr_subq_12.acquired_ds__week
                          , nr_subq_12.acquired_ds__month
                          , nr_subq_12.acquired_ds__quarter
                          , nr_subq_12.acquired_ds__year
                          , nr_subq_12.acquired_ds__extract_year
                          , nr_subq_12.acquired_ds__extract_quarter
                          , nr_subq_12.acquired_ds__extract_month
                          , nr_subq_12.acquired_ds__extract_day
                          , nr_subq_12.acquired_ds__extract_dow
                          , nr_subq_12.acquired_ds__extract_doy
                          , nr_subq_12.customer_id__acquired_ds__day
                          , nr_subq_12.customer_id__acquired_ds__week
                          , nr_subq_12.customer_id__acquired_ds__month
                          , nr_subq_12.customer_id__acquired_ds__quarter
                          , nr_subq_12.customer_id__acquired_ds__year
                          , nr_subq_12.customer_id__acquired_ds__extract_year
                          , nr_subq_12.customer_id__acquired_ds__extract_quarter
                          , nr_subq_12.customer_id__acquired_ds__extract_month
                          , nr_subq_12.customer_id__acquired_ds__extract_day
                          , nr_subq_12.customer_id__acquired_ds__extract_dow
                          , nr_subq_12.customer_id__acquired_ds__extract_doy
                          , nr_subq_12.customer_third_hop_id__acquired_ds__day
                          , nr_subq_12.customer_third_hop_id__acquired_ds__week
                          , nr_subq_12.customer_third_hop_id__acquired_ds__month
                          , nr_subq_12.customer_third_hop_id__acquired_ds__quarter
                          , nr_subq_12.customer_third_hop_id__acquired_ds__year
                          , nr_subq_12.customer_third_hop_id__acquired_ds__extract_year
                          , nr_subq_12.customer_third_hop_id__acquired_ds__extract_quarter
                          , nr_subq_12.customer_third_hop_id__acquired_ds__extract_month
                          , nr_subq_12.customer_third_hop_id__acquired_ds__extract_day
                          , nr_subq_12.customer_third_hop_id__acquired_ds__extract_dow
                          , nr_subq_12.customer_third_hop_id__acquired_ds__extract_doy
                          , nr_subq_12.metric_time__day
                          , nr_subq_12.metric_time__week
                          , nr_subq_12.metric_time__month
                          , nr_subq_12.metric_time__quarter
                          , nr_subq_12.metric_time__year
                          , nr_subq_12.metric_time__extract_year
                          , nr_subq_12.metric_time__extract_quarter
                          , nr_subq_12.metric_time__extract_month
                          , nr_subq_12.metric_time__extract_day
                          , nr_subq_12.metric_time__extract_dow
                          , nr_subq_12.metric_time__extract_doy
                          , nr_subq_12.customer_id
                          , nr_subq_12.customer_third_hop_id
                          , nr_subq_12.customer_id__customer_third_hop_id
                          , nr_subq_12.customer_third_hop_id__customer_id
                          , nr_subq_12.country
                          , nr_subq_12.customer_id__country
                          , nr_subq_12.customer_third_hop_id__country
                        FROM (
                          -- Metric Time Dimension 'acquired_ds'
                          SELECT
                            nr_subq_22002.acquired_ds__day
                            , nr_subq_22002.acquired_ds__week
                            , nr_subq_22002.acquired_ds__month
                            , nr_subq_22002.acquired_ds__quarter
                            , nr_subq_22002.acquired_ds__year
                            , nr_subq_22002.acquired_ds__extract_year
                            , nr_subq_22002.acquired_ds__extract_quarter
                            , nr_subq_22002.acquired_ds__extract_month
                            , nr_subq_22002.acquired_ds__extract_day
                            , nr_subq_22002.acquired_ds__extract_dow
                            , nr_subq_22002.acquired_ds__extract_doy
                            , nr_subq_22002.customer_id__acquired_ds__day
                            , nr_subq_22002.customer_id__acquired_ds__week
                            , nr_subq_22002.customer_id__acquired_ds__month
                            , nr_subq_22002.customer_id__acquired_ds__quarter
                            , nr_subq_22002.customer_id__acquired_ds__year
                            , nr_subq_22002.customer_id__acquired_ds__extract_year
                            , nr_subq_22002.customer_id__acquired_ds__extract_quarter
                            , nr_subq_22002.customer_id__acquired_ds__extract_month
                            , nr_subq_22002.customer_id__acquired_ds__extract_day
                            , nr_subq_22002.customer_id__acquired_ds__extract_dow
                            , nr_subq_22002.customer_id__acquired_ds__extract_doy
                            , nr_subq_22002.customer_third_hop_id__acquired_ds__day
                            , nr_subq_22002.customer_third_hop_id__acquired_ds__week
                            , nr_subq_22002.customer_third_hop_id__acquired_ds__month
                            , nr_subq_22002.customer_third_hop_id__acquired_ds__quarter
                            , nr_subq_22002.customer_third_hop_id__acquired_ds__year
                            , nr_subq_22002.customer_third_hop_id__acquired_ds__extract_year
                            , nr_subq_22002.customer_third_hop_id__acquired_ds__extract_quarter
                            , nr_subq_22002.customer_third_hop_id__acquired_ds__extract_month
                            , nr_subq_22002.customer_third_hop_id__acquired_ds__extract_day
                            , nr_subq_22002.customer_third_hop_id__acquired_ds__extract_dow
                            , nr_subq_22002.customer_third_hop_id__acquired_ds__extract_doy
                            , nr_subq_22002.acquired_ds__day AS metric_time__day
                            , nr_subq_22002.acquired_ds__week AS metric_time__week
                            , nr_subq_22002.acquired_ds__month AS metric_time__month
                            , nr_subq_22002.acquired_ds__quarter AS metric_time__quarter
                            , nr_subq_22002.acquired_ds__year AS metric_time__year
                            , nr_subq_22002.acquired_ds__extract_year AS metric_time__extract_year
                            , nr_subq_22002.acquired_ds__extract_quarter AS metric_time__extract_quarter
                            , nr_subq_22002.acquired_ds__extract_month AS metric_time__extract_month
                            , nr_subq_22002.acquired_ds__extract_day AS metric_time__extract_day
                            , nr_subq_22002.acquired_ds__extract_dow AS metric_time__extract_dow
                            , nr_subq_22002.acquired_ds__extract_doy AS metric_time__extract_doy
                            , nr_subq_22002.customer_id
                            , nr_subq_22002.customer_third_hop_id
                            , nr_subq_22002.customer_id__customer_third_hop_id
                            , nr_subq_22002.customer_third_hop_id__customer_id
                            , nr_subq_22002.country
                            , nr_subq_22002.customer_id__country
                            , nr_subq_22002.customer_third_hop_id__country
                            , nr_subq_22002.customers_with_other_data
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
                          ) nr_subq_22002
                        ) nr_subq_12
                      ) nr_subq_13
                      ON
                        nr_subq_11.customer_id = nr_subq_13.customer_id
                    ) nr_subq_14
                  ) nr_subq_15
                  ON
                    (
                      nr_subq_10.account_id = nr_subq_15.account_id
                    ) AND (
                      nr_subq_10.ds_partitioned__day = nr_subq_15.ds_partitioned__day
                    )
                ) nr_subq_16
              ) nr_subq_17
              GROUP BY
                account_id__customer_id__customer_third_hop_id
            ) nr_subq_18
          ) nr_subq_19
        ) nr_subq_20
        ON
          nr_subq_9.customer_third_hop_id = nr_subq_20.account_id__customer_id__customer_third_hop_id
      ) nr_subq_21
      WHERE customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count > 2
    ) nr_subq_22
  ) nr_subq_23
) nr_subq_24
