-- Compute Metrics via Expressions
SELECT
  subq_14.third_hop_count
FROM (
  -- Aggregate Measures
  SELECT
    COUNT(DISTINCT subq_13.third_hop_count) AS third_hop_count
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['third_hop_count',]
    SELECT
      subq_12.third_hop_count
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count']
      SELECT
        subq_1.third_hop_count AS third_hop_count
      FROM (
        -- Metric Time Dimension 'third_hop_ds'
        -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id']
        SELECT
          subq_0.customer_third_hop_id
          , subq_0.third_hop_count
        FROM (
          -- Read Elements From Semantic Model 'third_hop_table'
          SELECT
            third_hop_table_src_22000.customer_third_hop_id AS third_hop_count
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
            , EXTRACT(isodow FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_dow
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
            , EXTRACT(isodow FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_dow
            , EXTRACT(doy FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_doy
            , third_hop_table_src_22000.customer_third_hop_id
          FROM ***************************.third_hop_table third_hop_table_src_22000
        ) subq_0
      ) subq_1
      LEFT OUTER JOIN (
        -- Compute Metrics via Expressions
        -- Pass Only Elements: ['account_id__customer_id__customer_third_hop_id', 'account_id__customer_id__customer_third_hop_id__txn_count']
        SELECT
          subq_10.account_id__customer_id__customer_third_hop_id
          , subq_10.txn_count AS account_id__customer_id__customer_third_hop_id__txn_count
        FROM (
          -- Aggregate Measures
          SELECT
            subq_9.account_id__customer_id__customer_third_hop_id
            , SUM(subq_9.txn_count) AS txn_count
          FROM (
            -- Join Standard Outputs
            -- Pass Only Elements: ['txn_count', 'account_id__customer_id__customer_third_hop_id']
            SELECT
              subq_3.txn_count AS txn_count
            FROM (
              -- Metric Time Dimension 'ds'
              -- Pass Only Elements: ['txn_count', 'ds_partitioned__day', 'account_id']
              SELECT
                subq_2.ds_partitioned__day
                , subq_2.account_id
                , subq_2.txn_count
              FROM (
                -- Read Elements From Semantic Model 'account_month_txns'
                SELECT
                  account_month_txns_src_22000.txn_count
                  , DATE_TRUNC('day', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__day
                  , DATE_TRUNC('week', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__week
                  , DATE_TRUNC('month', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__month
                  , DATE_TRUNC('quarter', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__quarter
                  , DATE_TRUNC('year', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__year
                  , EXTRACT(year FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_year
                  , EXTRACT(quarter FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_quarter
                  , EXTRACT(month FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_month
                  , EXTRACT(day FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_day
                  , EXTRACT(isodow FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_dow
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
                  , EXTRACT(isodow FROM account_month_txns_src_22000.ds) AS ds__extract_dow
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
                  , EXTRACT(isodow FROM account_month_txns_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_dow
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
                  , EXTRACT(isodow FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_dow
                  , EXTRACT(doy FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_doy
                  , account_month_txns_src_22000.account_month AS account_id__account_month
                  , account_month_txns_src_22000.account_id
                FROM ***************************.account_month_txns account_month_txns_src_22000
              ) subq_2
            ) subq_3
            LEFT OUTER JOIN (
              -- Join Standard Outputs
              -- Pass Only Elements: ['ds_partitioned__day', 'account_id', 'customer_id__customer_third_hop_id']
              SELECT
                subq_5.ds_partitioned__day AS ds_partitioned__day
                , subq_5.account_id AS account_id
              FROM (
                -- Metric Time Dimension 'ds_partitioned'
                SELECT
                  subq_4.ds_partitioned__day
                  , subq_4.ds_partitioned__week
                  , subq_4.ds_partitioned__month
                  , subq_4.ds_partitioned__quarter
                  , subq_4.ds_partitioned__year
                  , subq_4.ds_partitioned__extract_year
                  , subq_4.ds_partitioned__extract_quarter
                  , subq_4.ds_partitioned__extract_month
                  , subq_4.ds_partitioned__extract_day
                  , subq_4.ds_partitioned__extract_dow
                  , subq_4.ds_partitioned__extract_doy
                  , subq_4.account_id__ds_partitioned__day
                  , subq_4.account_id__ds_partitioned__week
                  , subq_4.account_id__ds_partitioned__month
                  , subq_4.account_id__ds_partitioned__quarter
                  , subq_4.account_id__ds_partitioned__year
                  , subq_4.account_id__ds_partitioned__extract_year
                  , subq_4.account_id__ds_partitioned__extract_quarter
                  , subq_4.account_id__ds_partitioned__extract_month
                  , subq_4.account_id__ds_partitioned__extract_day
                  , subq_4.account_id__ds_partitioned__extract_dow
                  , subq_4.account_id__ds_partitioned__extract_doy
                  , subq_4.bridge_account__ds_partitioned__day
                  , subq_4.bridge_account__ds_partitioned__week
                  , subq_4.bridge_account__ds_partitioned__month
                  , subq_4.bridge_account__ds_partitioned__quarter
                  , subq_4.bridge_account__ds_partitioned__year
                  , subq_4.bridge_account__ds_partitioned__extract_year
                  , subq_4.bridge_account__ds_partitioned__extract_quarter
                  , subq_4.bridge_account__ds_partitioned__extract_month
                  , subq_4.bridge_account__ds_partitioned__extract_day
                  , subq_4.bridge_account__ds_partitioned__extract_dow
                  , subq_4.bridge_account__ds_partitioned__extract_doy
                  , subq_4.ds_partitioned__day AS metric_time__day
                  , subq_4.ds_partitioned__week AS metric_time__week
                  , subq_4.ds_partitioned__month AS metric_time__month
                  , subq_4.ds_partitioned__quarter AS metric_time__quarter
                  , subq_4.ds_partitioned__year AS metric_time__year
                  , subq_4.ds_partitioned__extract_year AS metric_time__extract_year
                  , subq_4.ds_partitioned__extract_quarter AS metric_time__extract_quarter
                  , subq_4.ds_partitioned__extract_month AS metric_time__extract_month
                  , subq_4.ds_partitioned__extract_day AS metric_time__extract_day
                  , subq_4.ds_partitioned__extract_dow AS metric_time__extract_dow
                  , subq_4.ds_partitioned__extract_doy AS metric_time__extract_doy
                  , subq_4.account_id
                  , subq_4.customer_id
                  , subq_4.account_id__customer_id
                  , subq_4.bridge_account__account_id
                  , subq_4.bridge_account__customer_id
                  , subq_4.extra_dim
                  , subq_4.account_id__extra_dim
                  , subq_4.bridge_account__extra_dim
                  , subq_4.account_customer_combos
                FROM (
                  -- Read Elements From Semantic Model 'bridge_table'
                  SELECT
                    account_id || customer_id AS account_customer_combos
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
                    , EXTRACT(isodow FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_dow
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
                    , EXTRACT(isodow FROM bridge_table_src_22000.ds_partitioned) AS account_id__ds_partitioned__extract_dow
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
                    , EXTRACT(isodow FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_dow
                    , EXTRACT(doy FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_doy
                    , bridge_table_src_22000.account_id
                    , bridge_table_src_22000.customer_id
                    , bridge_table_src_22000.customer_id AS account_id__customer_id
                    , bridge_table_src_22000.account_id AS bridge_account__account_id
                    , bridge_table_src_22000.customer_id AS bridge_account__customer_id
                  FROM ***************************.bridge_table bridge_table_src_22000
                ) subq_4
              ) subq_5
              LEFT OUTER JOIN (
                -- Metric Time Dimension 'acquired_ds'
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
                  subq_6.acquired_ds__day
                  , subq_6.acquired_ds__week
                  , subq_6.acquired_ds__month
                  , subq_6.acquired_ds__quarter
                  , subq_6.acquired_ds__year
                  , subq_6.acquired_ds__extract_year
                  , subq_6.acquired_ds__extract_quarter
                  , subq_6.acquired_ds__extract_month
                  , subq_6.acquired_ds__extract_day
                  , subq_6.acquired_ds__extract_dow
                  , subq_6.acquired_ds__extract_doy
                  , subq_6.customer_id__acquired_ds__day
                  , subq_6.customer_id__acquired_ds__week
                  , subq_6.customer_id__acquired_ds__month
                  , subq_6.customer_id__acquired_ds__quarter
                  , subq_6.customer_id__acquired_ds__year
                  , subq_6.customer_id__acquired_ds__extract_year
                  , subq_6.customer_id__acquired_ds__extract_quarter
                  , subq_6.customer_id__acquired_ds__extract_month
                  , subq_6.customer_id__acquired_ds__extract_day
                  , subq_6.customer_id__acquired_ds__extract_dow
                  , subq_6.customer_id__acquired_ds__extract_doy
                  , subq_6.customer_third_hop_id__acquired_ds__day
                  , subq_6.customer_third_hop_id__acquired_ds__week
                  , subq_6.customer_third_hop_id__acquired_ds__month
                  , subq_6.customer_third_hop_id__acquired_ds__quarter
                  , subq_6.customer_third_hop_id__acquired_ds__year
                  , subq_6.customer_third_hop_id__acquired_ds__extract_year
                  , subq_6.customer_third_hop_id__acquired_ds__extract_quarter
                  , subq_6.customer_third_hop_id__acquired_ds__extract_month
                  , subq_6.customer_third_hop_id__acquired_ds__extract_day
                  , subq_6.customer_third_hop_id__acquired_ds__extract_dow
                  , subq_6.customer_third_hop_id__acquired_ds__extract_doy
                  , subq_6.acquired_ds__day AS metric_time__day
                  , subq_6.acquired_ds__week AS metric_time__week
                  , subq_6.acquired_ds__month AS metric_time__month
                  , subq_6.acquired_ds__quarter AS metric_time__quarter
                  , subq_6.acquired_ds__year AS metric_time__year
                  , subq_6.acquired_ds__extract_year AS metric_time__extract_year
                  , subq_6.acquired_ds__extract_quarter AS metric_time__extract_quarter
                  , subq_6.acquired_ds__extract_month AS metric_time__extract_month
                  , subq_6.acquired_ds__extract_day AS metric_time__extract_day
                  , subq_6.acquired_ds__extract_dow AS metric_time__extract_dow
                  , subq_6.acquired_ds__extract_doy AS metric_time__extract_doy
                  , subq_6.customer_id
                  , subq_6.customer_third_hop_id
                  , subq_6.customer_id__customer_third_hop_id
                  , subq_6.customer_third_hop_id__customer_id
                  , subq_6.country
                  , subq_6.customer_id__country
                  , subq_6.customer_third_hop_id__country
                FROM (
                  -- Read Elements From Semantic Model 'customer_other_data'
                  SELECT
                    1 AS customers_with_other_data
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
                    , EXTRACT(isodow FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_dow
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
                    , EXTRACT(isodow FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_dow
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
                    , EXTRACT(isodow FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_dow
                    , EXTRACT(doy FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_doy
                    , customer_other_data_src_22000.customer_id
                    , customer_other_data_src_22000.customer_third_hop_id
                    , customer_other_data_src_22000.customer_third_hop_id AS customer_id__customer_third_hop_id
                    , customer_other_data_src_22000.customer_id AS customer_third_hop_id__customer_id
                  FROM ***************************.customer_other_data customer_other_data_src_22000
                ) subq_6
              ) subq_7
              ON
                subq_5.customer_id = subq_7.customer_id
            ) subq_8
            ON
              (
                subq_3.account_id = subq_8.account_id
              ) AND (
                subq_3.ds_partitioned__day = subq_8.ds_partitioned__day
              )
          ) subq_9
          GROUP BY
            subq_9.account_id__customer_id__customer_third_hop_id
        ) subq_10
      ) subq_11
      ON
        subq_1.customer_third_hop_id = subq_11.account_id__customer_id__customer_third_hop_id
    ) subq_12
    WHERE customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count > 2
  ) subq_13
) subq_14
