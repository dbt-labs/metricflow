-- Compute Metrics via Expressions
SELECT
  subq_17.account_id__customer_id__customer_name
  , subq_17.txn_count
FROM (
  -- Aggregate Measures
  SELECT
    subq_16.account_id__customer_id__customer_name
    , SUM(subq_16.txn_count) AS txn_count
  FROM (
    -- Pass Only Elements: ['txn_count', 'account_id__customer_id__customer_name']
    SELECT
      subq_15.account_id__customer_id__customer_name
      , subq_15.txn_count
    FROM (
      -- Join Standard Outputs
      SELECT
        subq_7.ds_partitioned__day AS ds_partitioned__day
        , subq_14.ds_partitioned__day AS account_id__ds_partitioned__day
        , subq_7.account_id AS account_id
        , subq_14.customer_id__customer_name AS account_id__customer_id__customer_name
        , subq_7.txn_count AS txn_count
      FROM (
        -- Pass Only Elements: ['txn_count', 'ds_partitioned__day', 'account_id']
        SELECT
          subq_6.ds_partitioned__day
          , subq_6.account_id
          , subq_6.txn_count
        FROM (
          -- Metric Time Dimension 'ds'
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
            , subq_5.ds__day
            , subq_5.ds__week
            , subq_5.ds__month
            , subq_5.ds__quarter
            , subq_5.ds__year
            , subq_5.ds__extract_year
            , subq_5.ds__extract_quarter
            , subq_5.ds__extract_month
            , subq_5.ds__extract_day
            , subq_5.ds__extract_dow
            , subq_5.ds__extract_doy
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
            , subq_5.account_id__ds__day
            , subq_5.account_id__ds__week
            , subq_5.account_id__ds__month
            , subq_5.account_id__ds__quarter
            , subq_5.account_id__ds__year
            , subq_5.account_id__ds__extract_year
            , subq_5.account_id__ds__extract_quarter
            , subq_5.account_id__ds__extract_month
            , subq_5.account_id__ds__extract_day
            , subq_5.account_id__ds__extract_dow
            , subq_5.account_id__ds__extract_doy
            , subq_5.ds__day AS metric_time__day
            , subq_5.ds__week AS metric_time__week
            , subq_5.ds__month AS metric_time__month
            , subq_5.ds__quarter AS metric_time__quarter
            , subq_5.ds__year AS metric_time__year
            , subq_5.ds__extract_year AS metric_time__extract_year
            , subq_5.ds__extract_quarter AS metric_time__extract_quarter
            , subq_5.ds__extract_month AS metric_time__extract_month
            , subq_5.ds__extract_day AS metric_time__extract_day
            , subq_5.ds__extract_dow AS metric_time__extract_dow
            , subq_5.ds__extract_doy AS metric_time__extract_doy
            , subq_5.account_id
            , subq_5.account_month
            , subq_5.account_id__account_month
            , subq_5.txn_count
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
          ) subq_5
        ) subq_6
      ) subq_7
      LEFT OUTER JOIN (
        -- Pass Only Elements: ['customer_id__customer_name', 'ds_partitioned__day', 'account_id']
        SELECT
          subq_13.ds_partitioned__day
          , subq_13.account_id
          , subq_13.customer_id__customer_name
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_9.ds_partitioned__day AS ds_partitioned__day
            , subq_9.ds_partitioned__week AS ds_partitioned__week
            , subq_9.ds_partitioned__month AS ds_partitioned__month
            , subq_9.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_9.ds_partitioned__year AS ds_partitioned__year
            , subq_9.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , subq_9.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , subq_9.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , subq_9.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , subq_9.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , subq_9.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , subq_9.account_id__ds_partitioned__day AS account_id__ds_partitioned__day
            , subq_9.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
            , subq_9.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
            , subq_9.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
            , subq_9.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
            , subq_9.account_id__ds_partitioned__extract_year AS account_id__ds_partitioned__extract_year
            , subq_9.account_id__ds_partitioned__extract_quarter AS account_id__ds_partitioned__extract_quarter
            , subq_9.account_id__ds_partitioned__extract_month AS account_id__ds_partitioned__extract_month
            , subq_9.account_id__ds_partitioned__extract_day AS account_id__ds_partitioned__extract_day
            , subq_9.account_id__ds_partitioned__extract_dow AS account_id__ds_partitioned__extract_dow
            , subq_9.account_id__ds_partitioned__extract_doy AS account_id__ds_partitioned__extract_doy
            , subq_9.bridge_account__ds_partitioned__day AS bridge_account__ds_partitioned__day
            , subq_9.bridge_account__ds_partitioned__week AS bridge_account__ds_partitioned__week
            , subq_9.bridge_account__ds_partitioned__month AS bridge_account__ds_partitioned__month
            , subq_9.bridge_account__ds_partitioned__quarter AS bridge_account__ds_partitioned__quarter
            , subq_9.bridge_account__ds_partitioned__year AS bridge_account__ds_partitioned__year
            , subq_9.bridge_account__ds_partitioned__extract_year AS bridge_account__ds_partitioned__extract_year
            , subq_9.bridge_account__ds_partitioned__extract_quarter AS bridge_account__ds_partitioned__extract_quarter
            , subq_9.bridge_account__ds_partitioned__extract_month AS bridge_account__ds_partitioned__extract_month
            , subq_9.bridge_account__ds_partitioned__extract_day AS bridge_account__ds_partitioned__extract_day
            , subq_9.bridge_account__ds_partitioned__extract_dow AS bridge_account__ds_partitioned__extract_dow
            , subq_9.bridge_account__ds_partitioned__extract_doy AS bridge_account__ds_partitioned__extract_doy
            , subq_9.metric_time__day AS metric_time__day
            , subq_9.metric_time__week AS metric_time__week
            , subq_9.metric_time__month AS metric_time__month
            , subq_9.metric_time__quarter AS metric_time__quarter
            , subq_9.metric_time__year AS metric_time__year
            , subq_9.metric_time__extract_year AS metric_time__extract_year
            , subq_9.metric_time__extract_quarter AS metric_time__extract_quarter
            , subq_9.metric_time__extract_month AS metric_time__extract_month
            , subq_9.metric_time__extract_day AS metric_time__extract_day
            , subq_9.metric_time__extract_dow AS metric_time__extract_dow
            , subq_9.metric_time__extract_doy AS metric_time__extract_doy
            , subq_12.ds_partitioned__day AS customer_id__ds_partitioned__day
            , subq_12.ds_partitioned__week AS customer_id__ds_partitioned__week
            , subq_12.ds_partitioned__month AS customer_id__ds_partitioned__month
            , subq_12.ds_partitioned__quarter AS customer_id__ds_partitioned__quarter
            , subq_12.ds_partitioned__year AS customer_id__ds_partitioned__year
            , subq_12.ds_partitioned__extract_year AS customer_id__ds_partitioned__extract_year
            , subq_12.ds_partitioned__extract_quarter AS customer_id__ds_partitioned__extract_quarter
            , subq_12.ds_partitioned__extract_month AS customer_id__ds_partitioned__extract_month
            , subq_12.ds_partitioned__extract_day AS customer_id__ds_partitioned__extract_day
            , subq_12.ds_partitioned__extract_dow AS customer_id__ds_partitioned__extract_dow
            , subq_12.ds_partitioned__extract_doy AS customer_id__ds_partitioned__extract_doy
            , subq_12.metric_time__day AS customer_id__metric_time__day
            , subq_12.metric_time__week AS customer_id__metric_time__week
            , subq_12.metric_time__month AS customer_id__metric_time__month
            , subq_12.metric_time__quarter AS customer_id__metric_time__quarter
            , subq_12.metric_time__year AS customer_id__metric_time__year
            , subq_12.metric_time__extract_year AS customer_id__metric_time__extract_year
            , subq_12.metric_time__extract_quarter AS customer_id__metric_time__extract_quarter
            , subq_12.metric_time__extract_month AS customer_id__metric_time__extract_month
            , subq_12.metric_time__extract_day AS customer_id__metric_time__extract_day
            , subq_12.metric_time__extract_dow AS customer_id__metric_time__extract_dow
            , subq_12.metric_time__extract_doy AS customer_id__metric_time__extract_doy
            , subq_9.account_id AS account_id
            , subq_9.customer_id AS customer_id
            , subq_9.account_id__customer_id AS account_id__customer_id
            , subq_9.bridge_account__account_id AS bridge_account__account_id
            , subq_9.bridge_account__customer_id AS bridge_account__customer_id
            , subq_9.extra_dim AS extra_dim
            , subq_9.account_id__extra_dim AS account_id__extra_dim
            , subq_9.bridge_account__extra_dim AS bridge_account__extra_dim
            , subq_12.customer_name AS customer_id__customer_name
            , subq_12.customer_atomic_weight AS customer_id__customer_atomic_weight
            , subq_9.account_customer_combos AS account_customer_combos
          FROM (
            -- Metric Time Dimension 'ds_partitioned'
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
              , subq_8.account_id__ds_partitioned__day
              , subq_8.account_id__ds_partitioned__week
              , subq_8.account_id__ds_partitioned__month
              , subq_8.account_id__ds_partitioned__quarter
              , subq_8.account_id__ds_partitioned__year
              , subq_8.account_id__ds_partitioned__extract_year
              , subq_8.account_id__ds_partitioned__extract_quarter
              , subq_8.account_id__ds_partitioned__extract_month
              , subq_8.account_id__ds_partitioned__extract_day
              , subq_8.account_id__ds_partitioned__extract_dow
              , subq_8.account_id__ds_partitioned__extract_doy
              , subq_8.bridge_account__ds_partitioned__day
              , subq_8.bridge_account__ds_partitioned__week
              , subq_8.bridge_account__ds_partitioned__month
              , subq_8.bridge_account__ds_partitioned__quarter
              , subq_8.bridge_account__ds_partitioned__year
              , subq_8.bridge_account__ds_partitioned__extract_year
              , subq_8.bridge_account__ds_partitioned__extract_quarter
              , subq_8.bridge_account__ds_partitioned__extract_month
              , subq_8.bridge_account__ds_partitioned__extract_day
              , subq_8.bridge_account__ds_partitioned__extract_dow
              , subq_8.bridge_account__ds_partitioned__extract_doy
              , subq_8.ds_partitioned__day AS metric_time__day
              , subq_8.ds_partitioned__week AS metric_time__week
              , subq_8.ds_partitioned__month AS metric_time__month
              , subq_8.ds_partitioned__quarter AS metric_time__quarter
              , subq_8.ds_partitioned__year AS metric_time__year
              , subq_8.ds_partitioned__extract_year AS metric_time__extract_year
              , subq_8.ds_partitioned__extract_quarter AS metric_time__extract_quarter
              , subq_8.ds_partitioned__extract_month AS metric_time__extract_month
              , subq_8.ds_partitioned__extract_day AS metric_time__extract_day
              , subq_8.ds_partitioned__extract_dow AS metric_time__extract_dow
              , subq_8.ds_partitioned__extract_doy AS metric_time__extract_doy
              , subq_8.account_id
              , subq_8.customer_id
              , subq_8.account_id__customer_id
              , subq_8.bridge_account__account_id
              , subq_8.bridge_account__customer_id
              , subq_8.extra_dim
              , subq_8.account_id__extra_dim
              , subq_8.bridge_account__extra_dim
              , subq_8.account_customer_combos
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
            ) subq_8
          ) subq_9
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
              subq_11.ds_partitioned__day
              , subq_11.ds_partitioned__week
              , subq_11.ds_partitioned__month
              , subq_11.ds_partitioned__quarter
              , subq_11.ds_partitioned__year
              , subq_11.ds_partitioned__extract_year
              , subq_11.ds_partitioned__extract_quarter
              , subq_11.ds_partitioned__extract_month
              , subq_11.ds_partitioned__extract_day
              , subq_11.ds_partitioned__extract_dow
              , subq_11.ds_partitioned__extract_doy
              , subq_11.customer_id__ds_partitioned__day
              , subq_11.customer_id__ds_partitioned__week
              , subq_11.customer_id__ds_partitioned__month
              , subq_11.customer_id__ds_partitioned__quarter
              , subq_11.customer_id__ds_partitioned__year
              , subq_11.customer_id__ds_partitioned__extract_year
              , subq_11.customer_id__ds_partitioned__extract_quarter
              , subq_11.customer_id__ds_partitioned__extract_month
              , subq_11.customer_id__ds_partitioned__extract_day
              , subq_11.customer_id__ds_partitioned__extract_dow
              , subq_11.customer_id__ds_partitioned__extract_doy
              , subq_11.metric_time__day
              , subq_11.metric_time__week
              , subq_11.metric_time__month
              , subq_11.metric_time__quarter
              , subq_11.metric_time__year
              , subq_11.metric_time__extract_year
              , subq_11.metric_time__extract_quarter
              , subq_11.metric_time__extract_month
              , subq_11.metric_time__extract_day
              , subq_11.metric_time__extract_dow
              , subq_11.metric_time__extract_doy
              , subq_11.customer_id
              , subq_11.customer_name
              , subq_11.customer_atomic_weight
              , subq_11.customer_id__customer_name
              , subq_11.customer_id__customer_atomic_weight
            FROM (
              -- Metric Time Dimension 'ds_partitioned'
              SELECT
                subq_10.ds_partitioned__day
                , subq_10.ds_partitioned__week
                , subq_10.ds_partitioned__month
                , subq_10.ds_partitioned__quarter
                , subq_10.ds_partitioned__year
                , subq_10.ds_partitioned__extract_year
                , subq_10.ds_partitioned__extract_quarter
                , subq_10.ds_partitioned__extract_month
                , subq_10.ds_partitioned__extract_day
                , subq_10.ds_partitioned__extract_dow
                , subq_10.ds_partitioned__extract_doy
                , subq_10.customer_id__ds_partitioned__day
                , subq_10.customer_id__ds_partitioned__week
                , subq_10.customer_id__ds_partitioned__month
                , subq_10.customer_id__ds_partitioned__quarter
                , subq_10.customer_id__ds_partitioned__year
                , subq_10.customer_id__ds_partitioned__extract_year
                , subq_10.customer_id__ds_partitioned__extract_quarter
                , subq_10.customer_id__ds_partitioned__extract_month
                , subq_10.customer_id__ds_partitioned__extract_day
                , subq_10.customer_id__ds_partitioned__extract_dow
                , subq_10.customer_id__ds_partitioned__extract_doy
                , subq_10.ds_partitioned__day AS metric_time__day
                , subq_10.ds_partitioned__week AS metric_time__week
                , subq_10.ds_partitioned__month AS metric_time__month
                , subq_10.ds_partitioned__quarter AS metric_time__quarter
                , subq_10.ds_partitioned__year AS metric_time__year
                , subq_10.ds_partitioned__extract_year AS metric_time__extract_year
                , subq_10.ds_partitioned__extract_quarter AS metric_time__extract_quarter
                , subq_10.ds_partitioned__extract_month AS metric_time__extract_month
                , subq_10.ds_partitioned__extract_day AS metric_time__extract_day
                , subq_10.ds_partitioned__extract_dow AS metric_time__extract_dow
                , subq_10.ds_partitioned__extract_doy AS metric_time__extract_doy
                , subq_10.customer_id
                , subq_10.customer_name
                , subq_10.customer_atomic_weight
                , subq_10.customer_id__customer_name
                , subq_10.customer_id__customer_atomic_weight
                , subq_10.customers
              FROM (
                -- Read Elements From Semantic Model 'customer_table'
                SELECT
                  1 AS customers
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
                  , EXTRACT(isodow FROM customer_table_src_22000.ds_partitioned) AS ds_partitioned__extract_dow
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
                  , EXTRACT(isodow FROM customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__extract_dow
                  , EXTRACT(doy FROM customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__extract_doy
                  , customer_table_src_22000.customer_id
                FROM ***************************.customer_table customer_table_src_22000
              ) subq_10
            ) subq_11
          ) subq_12
          ON
            (
              subq_9.customer_id = subq_12.customer_id
            ) AND (
              subq_9.ds_partitioned__day = subq_12.ds_partitioned__day
            )
        ) subq_13
      ) subq_14
      ON
        (
          subq_7.account_id = subq_14.account_id
        ) AND (
          subq_7.ds_partitioned__day = subq_14.ds_partitioned__day
        )
    ) subq_15
  ) subq_16
  GROUP BY
    subq_16.account_id__customer_id__customer_name
) subq_17
