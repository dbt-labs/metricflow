test_name: test_multihop_node
test_filename: test_query_rendering.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a join between 1 simple-metric input and 2 dimensions.
sql_engine: Trino
---
-- Write to DataTable
SELECT
  subq_16.account_id__customer_id__customer_name
  , subq_16.txn_count
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_15.account_id__customer_id__customer_name
    , subq_15.__txn_count AS txn_count
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_14.account_id__customer_id__customer_name
      , SUM(subq_14.__txn_count) AS __txn_count
    FROM (
      -- Pass Only Elements: ['__txn_count', 'account_id__customer_id__customer_name']
      SELECT
        subq_13.account_id__customer_id__customer_name
        , subq_13.__txn_count
      FROM (
        -- Pass Only Elements: ['__txn_count', 'account_id__customer_id__customer_name']
        SELECT
          subq_12.account_id__customer_id__customer_name
          , subq_12.__txn_count
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_11.customer_id__customer_name AS account_id__customer_id__customer_name
            , subq_11.ds_partitioned__day AS account_id__ds_partitioned__day
            , subq_4.ds_partitioned__day AS ds_partitioned__day
            , subq_4.ds_partitioned__week AS ds_partitioned__week
            , subq_4.ds_partitioned__month AS ds_partitioned__month
            , subq_4.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_4.ds_partitioned__year AS ds_partitioned__year
            , subq_4.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , subq_4.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , subq_4.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , subq_4.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , subq_4.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , subq_4.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , subq_4.ds__day AS ds__day
            , subq_4.ds__week AS ds__week
            , subq_4.ds__month AS ds__month
            , subq_4.ds__quarter AS ds__quarter
            , subq_4.ds__year AS ds__year
            , subq_4.ds__extract_year AS ds__extract_year
            , subq_4.ds__extract_quarter AS ds__extract_quarter
            , subq_4.ds__extract_month AS ds__extract_month
            , subq_4.ds__extract_day AS ds__extract_day
            , subq_4.ds__extract_dow AS ds__extract_dow
            , subq_4.ds__extract_doy AS ds__extract_doy
            , subq_4.account_id__ds_partitioned__day AS account_id__ds_partitioned__day
            , subq_4.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
            , subq_4.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
            , subq_4.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
            , subq_4.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
            , subq_4.account_id__ds_partitioned__extract_year AS account_id__ds_partitioned__extract_year
            , subq_4.account_id__ds_partitioned__extract_quarter AS account_id__ds_partitioned__extract_quarter
            , subq_4.account_id__ds_partitioned__extract_month AS account_id__ds_partitioned__extract_month
            , subq_4.account_id__ds_partitioned__extract_day AS account_id__ds_partitioned__extract_day
            , subq_4.account_id__ds_partitioned__extract_dow AS account_id__ds_partitioned__extract_dow
            , subq_4.account_id__ds_partitioned__extract_doy AS account_id__ds_partitioned__extract_doy
            , subq_4.account_id__ds__day AS account_id__ds__day
            , subq_4.account_id__ds__week AS account_id__ds__week
            , subq_4.account_id__ds__month AS account_id__ds__month
            , subq_4.account_id__ds__quarter AS account_id__ds__quarter
            , subq_4.account_id__ds__year AS account_id__ds__year
            , subq_4.account_id__ds__extract_year AS account_id__ds__extract_year
            , subq_4.account_id__ds__extract_quarter AS account_id__ds__extract_quarter
            , subq_4.account_id__ds__extract_month AS account_id__ds__extract_month
            , subq_4.account_id__ds__extract_day AS account_id__ds__extract_day
            , subq_4.account_id__ds__extract_dow AS account_id__ds__extract_dow
            , subq_4.account_id__ds__extract_doy AS account_id__ds__extract_doy
            , subq_4.metric_time__day AS metric_time__day
            , subq_4.metric_time__week AS metric_time__week
            , subq_4.metric_time__month AS metric_time__month
            , subq_4.metric_time__quarter AS metric_time__quarter
            , subq_4.metric_time__year AS metric_time__year
            , subq_4.metric_time__extract_year AS metric_time__extract_year
            , subq_4.metric_time__extract_quarter AS metric_time__extract_quarter
            , subq_4.metric_time__extract_month AS metric_time__extract_month
            , subq_4.metric_time__extract_day AS metric_time__extract_day
            , subq_4.metric_time__extract_dow AS metric_time__extract_dow
            , subq_4.metric_time__extract_doy AS metric_time__extract_doy
            , subq_4.account_id AS account_id
            , subq_4.account_month AS account_month
            , subq_4.account_id__account_month AS account_id__account_month
            , subq_4.__txn_count AS __txn_count
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
              , subq_3.__txn_count
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
            ) subq_3
          ) subq_4
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['customer_id__customer_name', 'ds_partitioned__day', 'account_id']
            SELECT
              subq_10.ds_partitioned__day
              , subq_10.account_id
              , subq_10.customer_id__customer_name
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_9.customer_name AS customer_id__customer_name
                , subq_9.customer_atomic_weight AS customer_id__customer_atomic_weight
                , subq_9.ds_partitioned__day AS customer_id__ds_partitioned__day
                , subq_9.ds_partitioned__week AS customer_id__ds_partitioned__week
                , subq_9.ds_partitioned__month AS customer_id__ds_partitioned__month
                , subq_9.ds_partitioned__quarter AS customer_id__ds_partitioned__quarter
                , subq_9.ds_partitioned__year AS customer_id__ds_partitioned__year
                , subq_9.ds_partitioned__extract_year AS customer_id__ds_partitioned__extract_year
                , subq_9.ds_partitioned__extract_quarter AS customer_id__ds_partitioned__extract_quarter
                , subq_9.ds_partitioned__extract_month AS customer_id__ds_partitioned__extract_month
                , subq_9.ds_partitioned__extract_day AS customer_id__ds_partitioned__extract_day
                , subq_9.ds_partitioned__extract_dow AS customer_id__ds_partitioned__extract_dow
                , subq_9.ds_partitioned__extract_doy AS customer_id__ds_partitioned__extract_doy
                , subq_9.metric_time__day AS customer_id__metric_time__day
                , subq_9.metric_time__week AS customer_id__metric_time__week
                , subq_9.metric_time__month AS customer_id__metric_time__month
                , subq_9.metric_time__quarter AS customer_id__metric_time__quarter
                , subq_9.metric_time__year AS customer_id__metric_time__year
                , subq_9.metric_time__extract_year AS customer_id__metric_time__extract_year
                , subq_9.metric_time__extract_quarter AS customer_id__metric_time__extract_quarter
                , subq_9.metric_time__extract_month AS customer_id__metric_time__extract_month
                , subq_9.metric_time__extract_day AS customer_id__metric_time__extract_day
                , subq_9.metric_time__extract_dow AS customer_id__metric_time__extract_dow
                , subq_9.metric_time__extract_doy AS customer_id__metric_time__extract_doy
                , subq_6.ds_partitioned__day AS ds_partitioned__day
                , subq_6.ds_partitioned__week AS ds_partitioned__week
                , subq_6.ds_partitioned__month AS ds_partitioned__month
                , subq_6.ds_partitioned__quarter AS ds_partitioned__quarter
                , subq_6.ds_partitioned__year AS ds_partitioned__year
                , subq_6.ds_partitioned__extract_year AS ds_partitioned__extract_year
                , subq_6.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                , subq_6.ds_partitioned__extract_month AS ds_partitioned__extract_month
                , subq_6.ds_partitioned__extract_day AS ds_partitioned__extract_day
                , subq_6.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                , subq_6.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                , subq_6.account_id__ds_partitioned__day AS account_id__ds_partitioned__day
                , subq_6.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
                , subq_6.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
                , subq_6.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
                , subq_6.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
                , subq_6.account_id__ds_partitioned__extract_year AS account_id__ds_partitioned__extract_year
                , subq_6.account_id__ds_partitioned__extract_quarter AS account_id__ds_partitioned__extract_quarter
                , subq_6.account_id__ds_partitioned__extract_month AS account_id__ds_partitioned__extract_month
                , subq_6.account_id__ds_partitioned__extract_day AS account_id__ds_partitioned__extract_day
                , subq_6.account_id__ds_partitioned__extract_dow AS account_id__ds_partitioned__extract_dow
                , subq_6.account_id__ds_partitioned__extract_doy AS account_id__ds_partitioned__extract_doy
                , subq_6.bridge_account__ds_partitioned__day AS bridge_account__ds_partitioned__day
                , subq_6.bridge_account__ds_partitioned__week AS bridge_account__ds_partitioned__week
                , subq_6.bridge_account__ds_partitioned__month AS bridge_account__ds_partitioned__month
                , subq_6.bridge_account__ds_partitioned__quarter AS bridge_account__ds_partitioned__quarter
                , subq_6.bridge_account__ds_partitioned__year AS bridge_account__ds_partitioned__year
                , subq_6.bridge_account__ds_partitioned__extract_year AS bridge_account__ds_partitioned__extract_year
                , subq_6.bridge_account__ds_partitioned__extract_quarter AS bridge_account__ds_partitioned__extract_quarter
                , subq_6.bridge_account__ds_partitioned__extract_month AS bridge_account__ds_partitioned__extract_month
                , subq_6.bridge_account__ds_partitioned__extract_day AS bridge_account__ds_partitioned__extract_day
                , subq_6.bridge_account__ds_partitioned__extract_dow AS bridge_account__ds_partitioned__extract_dow
                , subq_6.bridge_account__ds_partitioned__extract_doy AS bridge_account__ds_partitioned__extract_doy
                , subq_6.metric_time__day AS metric_time__day
                , subq_6.metric_time__week AS metric_time__week
                , subq_6.metric_time__month AS metric_time__month
                , subq_6.metric_time__quarter AS metric_time__quarter
                , subq_6.metric_time__year AS metric_time__year
                , subq_6.metric_time__extract_year AS metric_time__extract_year
                , subq_6.metric_time__extract_quarter AS metric_time__extract_quarter
                , subq_6.metric_time__extract_month AS metric_time__extract_month
                , subq_6.metric_time__extract_day AS metric_time__extract_day
                , subq_6.metric_time__extract_dow AS metric_time__extract_dow
                , subq_6.metric_time__extract_doy AS metric_time__extract_doy
                , subq_6.account_id AS account_id
                , subq_6.customer_id AS customer_id
                , subq_6.account_id__customer_id AS account_id__customer_id
                , subq_6.bridge_account__account_id AS bridge_account__account_id
                , subq_6.bridge_account__customer_id AS bridge_account__customer_id
                , subq_6.extra_dim AS extra_dim
                , subq_6.account_id__extra_dim AS account_id__extra_dim
                , subq_6.bridge_account__extra_dim AS bridge_account__extra_dim
                , subq_6.__account_customer_combos AS __account_customer_combos
              FROM (
                -- Metric Time Dimension 'ds_partitioned'
                SELECT
                  subq_5.ds_partitioned__day
                  , subq_5.ds_partitioned__week
                  , subq_5.ds_partitioned__month
                  , subq_5.ds_partitioned__quarter
                  , subq_5.ds_partitioned__year
                  , subq_5.ds_partitioned__extract_year
                  , subq_5.ds_partitioned__extract_quarter
                  , subq_5.ds_partitioned__extract_month
                  , subq_5.ds_partitioned__extract_day
                  , subq_5.ds_partitioned__extract_dow
                  , subq_5.ds_partitioned__extract_doy
                  , subq_5.account_id__ds_partitioned__day
                  , subq_5.account_id__ds_partitioned__week
                  , subq_5.account_id__ds_partitioned__month
                  , subq_5.account_id__ds_partitioned__quarter
                  , subq_5.account_id__ds_partitioned__year
                  , subq_5.account_id__ds_partitioned__extract_year
                  , subq_5.account_id__ds_partitioned__extract_quarter
                  , subq_5.account_id__ds_partitioned__extract_month
                  , subq_5.account_id__ds_partitioned__extract_day
                  , subq_5.account_id__ds_partitioned__extract_dow
                  , subq_5.account_id__ds_partitioned__extract_doy
                  , subq_5.bridge_account__ds_partitioned__day
                  , subq_5.bridge_account__ds_partitioned__week
                  , subq_5.bridge_account__ds_partitioned__month
                  , subq_5.bridge_account__ds_partitioned__quarter
                  , subq_5.bridge_account__ds_partitioned__year
                  , subq_5.bridge_account__ds_partitioned__extract_year
                  , subq_5.bridge_account__ds_partitioned__extract_quarter
                  , subq_5.bridge_account__ds_partitioned__extract_month
                  , subq_5.bridge_account__ds_partitioned__extract_day
                  , subq_5.bridge_account__ds_partitioned__extract_dow
                  , subq_5.bridge_account__ds_partitioned__extract_doy
                  , subq_5.ds_partitioned__day AS metric_time__day
                  , subq_5.ds_partitioned__week AS metric_time__week
                  , subq_5.ds_partitioned__month AS metric_time__month
                  , subq_5.ds_partitioned__quarter AS metric_time__quarter
                  , subq_5.ds_partitioned__year AS metric_time__year
                  , subq_5.ds_partitioned__extract_year AS metric_time__extract_year
                  , subq_5.ds_partitioned__extract_quarter AS metric_time__extract_quarter
                  , subq_5.ds_partitioned__extract_month AS metric_time__extract_month
                  , subq_5.ds_partitioned__extract_day AS metric_time__extract_day
                  , subq_5.ds_partitioned__extract_dow AS metric_time__extract_dow
                  , subq_5.ds_partitioned__extract_doy AS metric_time__extract_doy
                  , subq_5.account_id
                  , subq_5.customer_id
                  , subq_5.account_id__customer_id
                  , subq_5.bridge_account__account_id
                  , subq_5.bridge_account__customer_id
                  , subq_5.extra_dim
                  , subq_5.account_id__extra_dim
                  , subq_5.bridge_account__extra_dim
                  , subq_5.__account_customer_combos
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
                ) subq_5
              ) subq_6
              LEFT OUTER JOIN (
                -- Pass Only Elements: [
                --   'customer_name',
                --   'customer_atomic_weight',
                --   'customer_id__customer_name',
                --   'customer_id__customer_atomic_weight',
                --   'ds_partitioned__day',
                --   'ds_partitioned__week',
                --   'ds_partitioned__month',
                --   'ds_partitioned__quarter',
                --   'ds_partitioned__year',
                --   'ds_partitioned__extract_year',
                --   'ds_partitioned__extract_quarter',
                --   'ds_partitioned__extract_month',
                --   'ds_partitioned__extract_day',
                --   'ds_partitioned__extract_dow',
                --   'ds_partitioned__extract_doy',
                --   'customer_id__ds_partitioned__day',
                --   'customer_id__ds_partitioned__week',
                --   'customer_id__ds_partitioned__month',
                --   'customer_id__ds_partitioned__quarter',
                --   'customer_id__ds_partitioned__year',
                --   'customer_id__ds_partitioned__extract_year',
                --   'customer_id__ds_partitioned__extract_quarter',
                --   'customer_id__ds_partitioned__extract_month',
                --   'customer_id__ds_partitioned__extract_day',
                --   'customer_id__ds_partitioned__extract_dow',
                --   'customer_id__ds_partitioned__extract_doy',
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
                -- ]
                SELECT
                  subq_8.ds_partitioned__day
                  , subq_8.ds_partitioned__week
                  , subq_8.ds_partitioned__month
                  , subq_8.ds_partitioned__quarter
                  , subq_8.ds_partitioned__year
                  , subq_8.ds_partitioned__extract_year
                  , subq_8.ds_partitioned__extract_quarter
                  , subq_8.ds_partitioned__extract_month
                  , subq_8.ds_partitioned__extract_day
                  , subq_8.ds_partitioned__extract_dow
                  , subq_8.ds_partitioned__extract_doy
                  , subq_8.customer_id__ds_partitioned__day
                  , subq_8.customer_id__ds_partitioned__week
                  , subq_8.customer_id__ds_partitioned__month
                  , subq_8.customer_id__ds_partitioned__quarter
                  , subq_8.customer_id__ds_partitioned__year
                  , subq_8.customer_id__ds_partitioned__extract_year
                  , subq_8.customer_id__ds_partitioned__extract_quarter
                  , subq_8.customer_id__ds_partitioned__extract_month
                  , subq_8.customer_id__ds_partitioned__extract_day
                  , subq_8.customer_id__ds_partitioned__extract_dow
                  , subq_8.customer_id__ds_partitioned__extract_doy
                  , subq_8.metric_time__day
                  , subq_8.metric_time__week
                  , subq_8.metric_time__month
                  , subq_8.metric_time__quarter
                  , subq_8.metric_time__year
                  , subq_8.metric_time__extract_year
                  , subq_8.metric_time__extract_quarter
                  , subq_8.metric_time__extract_month
                  , subq_8.metric_time__extract_day
                  , subq_8.metric_time__extract_dow
                  , subq_8.metric_time__extract_doy
                  , subq_8.customer_id
                  , subq_8.customer_name
                  , subq_8.customer_atomic_weight
                  , subq_8.customer_id__customer_name
                  , subq_8.customer_id__customer_atomic_weight
                FROM (
                  -- Metric Time Dimension 'ds_partitioned'
                  SELECT
                    subq_7.ds_partitioned__day
                    , subq_7.ds_partitioned__week
                    , subq_7.ds_partitioned__month
                    , subq_7.ds_partitioned__quarter
                    , subq_7.ds_partitioned__year
                    , subq_7.ds_partitioned__extract_year
                    , subq_7.ds_partitioned__extract_quarter
                    , subq_7.ds_partitioned__extract_month
                    , subq_7.ds_partitioned__extract_day
                    , subq_7.ds_partitioned__extract_dow
                    , subq_7.ds_partitioned__extract_doy
                    , subq_7.customer_id__ds_partitioned__day
                    , subq_7.customer_id__ds_partitioned__week
                    , subq_7.customer_id__ds_partitioned__month
                    , subq_7.customer_id__ds_partitioned__quarter
                    , subq_7.customer_id__ds_partitioned__year
                    , subq_7.customer_id__ds_partitioned__extract_year
                    , subq_7.customer_id__ds_partitioned__extract_quarter
                    , subq_7.customer_id__ds_partitioned__extract_month
                    , subq_7.customer_id__ds_partitioned__extract_day
                    , subq_7.customer_id__ds_partitioned__extract_dow
                    , subq_7.customer_id__ds_partitioned__extract_doy
                    , subq_7.ds_partitioned__day AS metric_time__day
                    , subq_7.ds_partitioned__week AS metric_time__week
                    , subq_7.ds_partitioned__month AS metric_time__month
                    , subq_7.ds_partitioned__quarter AS metric_time__quarter
                    , subq_7.ds_partitioned__year AS metric_time__year
                    , subq_7.ds_partitioned__extract_year AS metric_time__extract_year
                    , subq_7.ds_partitioned__extract_quarter AS metric_time__extract_quarter
                    , subq_7.ds_partitioned__extract_month AS metric_time__extract_month
                    , subq_7.ds_partitioned__extract_day AS metric_time__extract_day
                    , subq_7.ds_partitioned__extract_dow AS metric_time__extract_dow
                    , subq_7.ds_partitioned__extract_doy AS metric_time__extract_doy
                    , subq_7.customer_id
                    , subq_7.customer_name
                    , subq_7.customer_atomic_weight
                    , subq_7.customer_id__customer_name
                    , subq_7.customer_id__customer_atomic_weight
                    , subq_7.__customers
                  FROM (
                    -- Read Elements From Semantic Model 'customer_table'
                    SELECT
                      1 AS __customers
                      , customer_table_src_22000.customer_name
                      , customer_table_src_22000.customer_atomic_weight
                      , DATE_TRUNC('day', customer_table_src_22000.ds_partitioned) AS ds_partitioned__day
                      , DATE_TRUNC('week', customer_table_src_22000.ds_partitioned) AS ds_partitioned__week
                      , DATE_TRUNC('month', customer_table_src_22000.ds_partitioned) AS ds_partitioned__month
                      , DATE_TRUNC('quarter', customer_table_src_22000.ds_partitioned) AS ds_partitioned__quarter
                      , DATE_TRUNC('year', customer_table_src_22000.ds_partitioned) AS ds_partitioned__year
                      , EXTRACT(year FROM customer_table_src_22000.ds_partitioned) AS ds_partitioned__extract_year
                      , EXTRACT(quarter FROM customer_table_src_22000.ds_partitioned) AS ds_partitioned__extract_quarter
                      , EXTRACT(month FROM customer_table_src_22000.ds_partitioned) AS ds_partitioned__extract_month
                      , EXTRACT(day FROM customer_table_src_22000.ds_partitioned) AS ds_partitioned__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM customer_table_src_22000.ds_partitioned) AS ds_partitioned__extract_dow
                      , EXTRACT(doy FROM customer_table_src_22000.ds_partitioned) AS ds_partitioned__extract_doy
                      , customer_table_src_22000.customer_name AS customer_id__customer_name
                      , customer_table_src_22000.customer_atomic_weight AS customer_id__customer_atomic_weight
                      , DATE_TRUNC('day', customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__day
                      , DATE_TRUNC('week', customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__week
                      , DATE_TRUNC('month', customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__month
                      , DATE_TRUNC('quarter', customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__quarter
                      , DATE_TRUNC('year', customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__year
                      , EXTRACT(year FROM customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__extract_year
                      , EXTRACT(quarter FROM customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__extract_quarter
                      , EXTRACT(month FROM customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__extract_month
                      , EXTRACT(day FROM customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__extract_dow
                      , EXTRACT(doy FROM customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__extract_doy
                      , customer_table_src_22000.customer_id
                    FROM ***************************.customer_table customer_table_src_22000
                  ) subq_7
                ) subq_8
              ) subq_9
              ON
                (
                  subq_6.customer_id = subq_9.customer_id
                ) AND (
                  subq_6.ds_partitioned__day = subq_9.ds_partitioned__day
                )
            ) subq_10
          ) subq_11
          ON
            (
              subq_4.account_id = subq_11.account_id
            ) AND (
              subq_4.ds_partitioned__day = subq_11.ds_partitioned__day
            )
        ) subq_12
      ) subq_13
    ) subq_14
    GROUP BY
      subq_14.account_id__customer_id__customer_name
  ) subq_15
) subq_16
