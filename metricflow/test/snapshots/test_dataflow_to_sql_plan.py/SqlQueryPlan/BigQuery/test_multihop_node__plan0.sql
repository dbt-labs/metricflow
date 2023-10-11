-- Compute Metrics via Expressions
SELECT
  subq_10.account_id__customer_id__customer_name
  , subq_10.txn_count
FROM (
  -- Aggregate Measures
  SELECT
    subq_9.account_id__customer_id__customer_name
    , SUM(subq_9.txn_count) AS txn_count
  FROM (
    -- Pass Only Elements:
    --   ['txn_count', 'account_id__customer_id__customer_name']
    SELECT
      subq_8.account_id__customer_id__customer_name
      , subq_8.txn_count
    FROM (
      -- Join Standard Outputs
      SELECT
        subq_2.ds_partitioned__day AS ds_partitioned__day
        , subq_7.ds_partitioned__day AS account_id__ds_partitioned__day
        , subq_2.account_id AS account_id
        , subq_7.customer_id__customer_name AS account_id__customer_id__customer_name
        , subq_2.txn_count AS txn_count
      FROM (
        -- Pass Only Elements:
        --   ['txn_count', 'ds_partitioned__day', 'account_id']
        SELECT
          subq_1.ds_partitioned__day
          , subq_1.account_id
          , subq_1.txn_count
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_0.ds_partitioned__day
            , subq_0.ds_partitioned__week
            , subq_0.ds_partitioned__month
            , subq_0.ds_partitioned__quarter
            , subq_0.ds_partitioned__year
            , subq_0.ds_partitioned__extract_year
            , subq_0.ds_partitioned__extract_quarter
            , subq_0.ds_partitioned__extract_month
            , subq_0.ds_partitioned__extract_week
            , subq_0.ds_partitioned__extract_day
            , subq_0.ds_partitioned__extract_dow
            , subq_0.ds_partitioned__extract_doy
            , subq_0.ds__day
            , subq_0.ds__week
            , subq_0.ds__month
            , subq_0.ds__quarter
            , subq_0.ds__year
            , subq_0.ds__extract_year
            , subq_0.ds__extract_quarter
            , subq_0.ds__extract_month
            , subq_0.ds__extract_week
            , subq_0.ds__extract_day
            , subq_0.ds__extract_dow
            , subq_0.ds__extract_doy
            , subq_0.account_id__ds_partitioned__day
            , subq_0.account_id__ds_partitioned__week
            , subq_0.account_id__ds_partitioned__month
            , subq_0.account_id__ds_partitioned__quarter
            , subq_0.account_id__ds_partitioned__year
            , subq_0.account_id__ds_partitioned__extract_year
            , subq_0.account_id__ds_partitioned__extract_quarter
            , subq_0.account_id__ds_partitioned__extract_month
            , subq_0.account_id__ds_partitioned__extract_week
            , subq_0.account_id__ds_partitioned__extract_day
            , subq_0.account_id__ds_partitioned__extract_dow
            , subq_0.account_id__ds_partitioned__extract_doy
            , subq_0.account_id__ds__day
            , subq_0.account_id__ds__week
            , subq_0.account_id__ds__month
            , subq_0.account_id__ds__quarter
            , subq_0.account_id__ds__year
            , subq_0.account_id__ds__extract_year
            , subq_0.account_id__ds__extract_quarter
            , subq_0.account_id__ds__extract_month
            , subq_0.account_id__ds__extract_week
            , subq_0.account_id__ds__extract_day
            , subq_0.account_id__ds__extract_dow
            , subq_0.account_id__ds__extract_doy
            , subq_0.ds__day AS metric_time__day
            , subq_0.ds__week AS metric_time__week
            , subq_0.ds__month AS metric_time__month
            , subq_0.ds__quarter AS metric_time__quarter
            , subq_0.ds__year AS metric_time__year
            , subq_0.ds__extract_year AS metric_time__extract_year
            , subq_0.ds__extract_quarter AS metric_time__extract_quarter
            , subq_0.ds__extract_month AS metric_time__extract_month
            , subq_0.ds__extract_week AS metric_time__extract_week
            , subq_0.ds__extract_day AS metric_time__extract_day
            , subq_0.ds__extract_dow AS metric_time__extract_dow
            , subq_0.ds__extract_doy AS metric_time__extract_doy
            , subq_0.account_id
            , subq_0.account_month
            , subq_0.account_id__account_month
            , subq_0.txn_count
          FROM (
            -- Read Elements From Semantic Model 'account_month_txns'
            SELECT
              account_month_txns_src_10010.txn_count
              , DATE_TRUNC(account_month_txns_src_10010.ds_partitioned, day) AS ds_partitioned__day
              , DATE_TRUNC(account_month_txns_src_10010.ds_partitioned, isoweek) AS ds_partitioned__week
              , DATE_TRUNC(account_month_txns_src_10010.ds_partitioned, month) AS ds_partitioned__month
              , DATE_TRUNC(account_month_txns_src_10010.ds_partitioned, quarter) AS ds_partitioned__quarter
              , DATE_TRUNC(account_month_txns_src_10010.ds_partitioned, year) AS ds_partitioned__year
              , EXTRACT(year FROM account_month_txns_src_10010.ds_partitioned) AS ds_partitioned__extract_year
              , EXTRACT(quarter FROM account_month_txns_src_10010.ds_partitioned) AS ds_partitioned__extract_quarter
              , EXTRACT(month FROM account_month_txns_src_10010.ds_partitioned) AS ds_partitioned__extract_month
              , EXTRACT(isoweek FROM account_month_txns_src_10010.ds_partitioned) AS ds_partitioned__extract_week
              , EXTRACT(day FROM account_month_txns_src_10010.ds_partitioned) AS ds_partitioned__extract_day
              , EXTRACT(dayofweek FROM account_month_txns_src_10010.ds_partitioned) AS ds_partitioned__extract_dow
              , EXTRACT(dayofyear FROM account_month_txns_src_10010.ds_partitioned) AS ds_partitioned__extract_doy
              , DATE_TRUNC(account_month_txns_src_10010.ds, day) AS ds__day
              , DATE_TRUNC(account_month_txns_src_10010.ds, isoweek) AS ds__week
              , DATE_TRUNC(account_month_txns_src_10010.ds, month) AS ds__month
              , DATE_TRUNC(account_month_txns_src_10010.ds, quarter) AS ds__quarter
              , DATE_TRUNC(account_month_txns_src_10010.ds, year) AS ds__year
              , EXTRACT(year FROM account_month_txns_src_10010.ds) AS ds__extract_year
              , EXTRACT(quarter FROM account_month_txns_src_10010.ds) AS ds__extract_quarter
              , EXTRACT(month FROM account_month_txns_src_10010.ds) AS ds__extract_month
              , EXTRACT(isoweek FROM account_month_txns_src_10010.ds) AS ds__extract_week
              , EXTRACT(day FROM account_month_txns_src_10010.ds) AS ds__extract_day
              , EXTRACT(dayofweek FROM account_month_txns_src_10010.ds) AS ds__extract_dow
              , EXTRACT(dayofyear FROM account_month_txns_src_10010.ds) AS ds__extract_doy
              , account_month_txns_src_10010.account_month
              , DATE_TRUNC(account_month_txns_src_10010.ds_partitioned, day) AS account_id__ds_partitioned__day
              , DATE_TRUNC(account_month_txns_src_10010.ds_partitioned, isoweek) AS account_id__ds_partitioned__week
              , DATE_TRUNC(account_month_txns_src_10010.ds_partitioned, month) AS account_id__ds_partitioned__month
              , DATE_TRUNC(account_month_txns_src_10010.ds_partitioned, quarter) AS account_id__ds_partitioned__quarter
              , DATE_TRUNC(account_month_txns_src_10010.ds_partitioned, year) AS account_id__ds_partitioned__year
              , EXTRACT(year FROM account_month_txns_src_10010.ds_partitioned) AS account_id__ds_partitioned__extract_year
              , EXTRACT(quarter FROM account_month_txns_src_10010.ds_partitioned) AS account_id__ds_partitioned__extract_quarter
              , EXTRACT(month FROM account_month_txns_src_10010.ds_partitioned) AS account_id__ds_partitioned__extract_month
              , EXTRACT(isoweek FROM account_month_txns_src_10010.ds_partitioned) AS account_id__ds_partitioned__extract_week
              , EXTRACT(day FROM account_month_txns_src_10010.ds_partitioned) AS account_id__ds_partitioned__extract_day
              , EXTRACT(dayofweek FROM account_month_txns_src_10010.ds_partitioned) AS account_id__ds_partitioned__extract_dow
              , EXTRACT(dayofyear FROM account_month_txns_src_10010.ds_partitioned) AS account_id__ds_partitioned__extract_doy
              , DATE_TRUNC(account_month_txns_src_10010.ds, day) AS account_id__ds__day
              , DATE_TRUNC(account_month_txns_src_10010.ds, isoweek) AS account_id__ds__week
              , DATE_TRUNC(account_month_txns_src_10010.ds, month) AS account_id__ds__month
              , DATE_TRUNC(account_month_txns_src_10010.ds, quarter) AS account_id__ds__quarter
              , DATE_TRUNC(account_month_txns_src_10010.ds, year) AS account_id__ds__year
              , EXTRACT(year FROM account_month_txns_src_10010.ds) AS account_id__ds__extract_year
              , EXTRACT(quarter FROM account_month_txns_src_10010.ds) AS account_id__ds__extract_quarter
              , EXTRACT(month FROM account_month_txns_src_10010.ds) AS account_id__ds__extract_month
              , EXTRACT(isoweek FROM account_month_txns_src_10010.ds) AS account_id__ds__extract_week
              , EXTRACT(day FROM account_month_txns_src_10010.ds) AS account_id__ds__extract_day
              , EXTRACT(dayofweek FROM account_month_txns_src_10010.ds) AS account_id__ds__extract_dow
              , EXTRACT(dayofyear FROM account_month_txns_src_10010.ds) AS account_id__ds__extract_doy
              , account_month_txns_src_10010.account_month AS account_id__account_month
              , account_month_txns_src_10010.account_id
            FROM ***************************.account_month_txns account_month_txns_src_10010
          ) subq_0
        ) subq_1
      ) subq_2
      LEFT OUTER JOIN (
        -- Pass Only Elements:
        --   ['customer_id__customer_name', 'ds_partitioned__day', 'account_id']
        SELECT
          subq_6.ds_partitioned__day
          , subq_6.account_id
          , subq_6.customer_id__customer_name
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_3.ds_partitioned__day AS ds_partitioned__day
            , subq_3.ds_partitioned__week AS ds_partitioned__week
            , subq_3.ds_partitioned__month AS ds_partitioned__month
            , subq_3.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_3.ds_partitioned__year AS ds_partitioned__year
            , subq_3.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , subq_3.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , subq_3.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , subq_3.ds_partitioned__extract_week AS ds_partitioned__extract_week
            , subq_3.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , subq_3.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , subq_3.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , subq_3.account_id__ds_partitioned__day AS account_id__ds_partitioned__day
            , subq_3.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
            , subq_3.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
            , subq_3.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
            , subq_3.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
            , subq_3.account_id__ds_partitioned__extract_year AS account_id__ds_partitioned__extract_year
            , subq_3.account_id__ds_partitioned__extract_quarter AS account_id__ds_partitioned__extract_quarter
            , subq_3.account_id__ds_partitioned__extract_month AS account_id__ds_partitioned__extract_month
            , subq_3.account_id__ds_partitioned__extract_week AS account_id__ds_partitioned__extract_week
            , subq_3.account_id__ds_partitioned__extract_day AS account_id__ds_partitioned__extract_day
            , subq_3.account_id__ds_partitioned__extract_dow AS account_id__ds_partitioned__extract_dow
            , subq_3.account_id__ds_partitioned__extract_doy AS account_id__ds_partitioned__extract_doy
            , subq_3.bridge_account__ds_partitioned__day AS bridge_account__ds_partitioned__day
            , subq_3.bridge_account__ds_partitioned__week AS bridge_account__ds_partitioned__week
            , subq_3.bridge_account__ds_partitioned__month AS bridge_account__ds_partitioned__month
            , subq_3.bridge_account__ds_partitioned__quarter AS bridge_account__ds_partitioned__quarter
            , subq_3.bridge_account__ds_partitioned__year AS bridge_account__ds_partitioned__year
            , subq_3.bridge_account__ds_partitioned__extract_year AS bridge_account__ds_partitioned__extract_year
            , subq_3.bridge_account__ds_partitioned__extract_quarter AS bridge_account__ds_partitioned__extract_quarter
            , subq_3.bridge_account__ds_partitioned__extract_month AS bridge_account__ds_partitioned__extract_month
            , subq_3.bridge_account__ds_partitioned__extract_week AS bridge_account__ds_partitioned__extract_week
            , subq_3.bridge_account__ds_partitioned__extract_day AS bridge_account__ds_partitioned__extract_day
            , subq_3.bridge_account__ds_partitioned__extract_dow AS bridge_account__ds_partitioned__extract_dow
            , subq_3.bridge_account__ds_partitioned__extract_doy AS bridge_account__ds_partitioned__extract_doy
            , subq_5.ds_partitioned__day AS customer_id__ds_partitioned__day
            , subq_5.ds_partitioned__week AS customer_id__ds_partitioned__week
            , subq_5.ds_partitioned__month AS customer_id__ds_partitioned__month
            , subq_5.ds_partitioned__quarter AS customer_id__ds_partitioned__quarter
            , subq_5.ds_partitioned__year AS customer_id__ds_partitioned__year
            , subq_5.ds_partitioned__extract_year AS customer_id__ds_partitioned__extract_year
            , subq_5.ds_partitioned__extract_quarter AS customer_id__ds_partitioned__extract_quarter
            , subq_5.ds_partitioned__extract_month AS customer_id__ds_partitioned__extract_month
            , subq_5.ds_partitioned__extract_week AS customer_id__ds_partitioned__extract_week
            , subq_5.ds_partitioned__extract_day AS customer_id__ds_partitioned__extract_day
            , subq_5.ds_partitioned__extract_dow AS customer_id__ds_partitioned__extract_dow
            , subq_5.ds_partitioned__extract_doy AS customer_id__ds_partitioned__extract_doy
            , subq_3.account_id AS account_id
            , subq_3.customer_id AS customer_id
            , subq_3.account_id__customer_id AS account_id__customer_id
            , subq_3.bridge_account__account_id AS bridge_account__account_id
            , subq_3.bridge_account__customer_id AS bridge_account__customer_id
            , subq_3.extra_dim AS extra_dim
            , subq_3.account_id__extra_dim AS account_id__extra_dim
            , subq_3.bridge_account__extra_dim AS bridge_account__extra_dim
            , subq_5.customer_name AS customer_id__customer_name
            , subq_5.customer_atomic_weight AS customer_id__customer_atomic_weight
          FROM (
            -- Read Elements From Semantic Model 'bridge_table'
            SELECT
              bridge_table_src_10011.extra_dim
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, day) AS ds_partitioned__day
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, isoweek) AS ds_partitioned__week
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, month) AS ds_partitioned__month
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, quarter) AS ds_partitioned__quarter
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, year) AS ds_partitioned__year
              , EXTRACT(year FROM bridge_table_src_10011.ds_partitioned) AS ds_partitioned__extract_year
              , EXTRACT(quarter FROM bridge_table_src_10011.ds_partitioned) AS ds_partitioned__extract_quarter
              , EXTRACT(month FROM bridge_table_src_10011.ds_partitioned) AS ds_partitioned__extract_month
              , EXTRACT(isoweek FROM bridge_table_src_10011.ds_partitioned) AS ds_partitioned__extract_week
              , EXTRACT(day FROM bridge_table_src_10011.ds_partitioned) AS ds_partitioned__extract_day
              , EXTRACT(dayofweek FROM bridge_table_src_10011.ds_partitioned) AS ds_partitioned__extract_dow
              , EXTRACT(dayofyear FROM bridge_table_src_10011.ds_partitioned) AS ds_partitioned__extract_doy
              , bridge_table_src_10011.extra_dim AS account_id__extra_dim
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, day) AS account_id__ds_partitioned__day
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, isoweek) AS account_id__ds_partitioned__week
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, month) AS account_id__ds_partitioned__month
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, quarter) AS account_id__ds_partitioned__quarter
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, year) AS account_id__ds_partitioned__year
              , EXTRACT(year FROM bridge_table_src_10011.ds_partitioned) AS account_id__ds_partitioned__extract_year
              , EXTRACT(quarter FROM bridge_table_src_10011.ds_partitioned) AS account_id__ds_partitioned__extract_quarter
              , EXTRACT(month FROM bridge_table_src_10011.ds_partitioned) AS account_id__ds_partitioned__extract_month
              , EXTRACT(isoweek FROM bridge_table_src_10011.ds_partitioned) AS account_id__ds_partitioned__extract_week
              , EXTRACT(day FROM bridge_table_src_10011.ds_partitioned) AS account_id__ds_partitioned__extract_day
              , EXTRACT(dayofweek FROM bridge_table_src_10011.ds_partitioned) AS account_id__ds_partitioned__extract_dow
              , EXTRACT(dayofyear FROM bridge_table_src_10011.ds_partitioned) AS account_id__ds_partitioned__extract_doy
              , bridge_table_src_10011.extra_dim AS bridge_account__extra_dim
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, day) AS bridge_account__ds_partitioned__day
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, isoweek) AS bridge_account__ds_partitioned__week
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, month) AS bridge_account__ds_partitioned__month
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, quarter) AS bridge_account__ds_partitioned__quarter
              , DATE_TRUNC(bridge_table_src_10011.ds_partitioned, year) AS bridge_account__ds_partitioned__year
              , EXTRACT(year FROM bridge_table_src_10011.ds_partitioned) AS bridge_account__ds_partitioned__extract_year
              , EXTRACT(quarter FROM bridge_table_src_10011.ds_partitioned) AS bridge_account__ds_partitioned__extract_quarter
              , EXTRACT(month FROM bridge_table_src_10011.ds_partitioned) AS bridge_account__ds_partitioned__extract_month
              , EXTRACT(isoweek FROM bridge_table_src_10011.ds_partitioned) AS bridge_account__ds_partitioned__extract_week
              , EXTRACT(day FROM bridge_table_src_10011.ds_partitioned) AS bridge_account__ds_partitioned__extract_day
              , EXTRACT(dayofweek FROM bridge_table_src_10011.ds_partitioned) AS bridge_account__ds_partitioned__extract_dow
              , EXTRACT(dayofyear FROM bridge_table_src_10011.ds_partitioned) AS bridge_account__ds_partitioned__extract_doy
              , bridge_table_src_10011.account_id
              , bridge_table_src_10011.customer_id
              , bridge_table_src_10011.customer_id AS account_id__customer_id
              , bridge_table_src_10011.account_id AS bridge_account__account_id
              , bridge_table_src_10011.customer_id AS bridge_account__customer_id
            FROM ***************************.bridge_table bridge_table_src_10011
          ) subq_3
          LEFT OUTER JOIN (
            -- Pass Only Elements:
            --   ['customer_name',
            --    'customer_atomic_weight',
            --    'customer_id__customer_name',
            --    'customer_id__customer_atomic_weight',
            --    'ds_partitioned__day',
            --    'ds_partitioned__week',
            --    'ds_partitioned__month',
            --    'ds_partitioned__quarter',
            --    'ds_partitioned__year',
            --    'ds_partitioned__extract_year',
            --    'ds_partitioned__extract_quarter',
            --    'ds_partitioned__extract_month',
            --    'ds_partitioned__extract_week',
            --    'ds_partitioned__extract_day',
            --    'ds_partitioned__extract_dow',
            --    'ds_partitioned__extract_doy',
            --    'customer_id__ds_partitioned__day',
            --    'customer_id__ds_partitioned__week',
            --    'customer_id__ds_partitioned__month',
            --    'customer_id__ds_partitioned__quarter',
            --    'customer_id__ds_partitioned__year',
            --    'customer_id__ds_partitioned__extract_year',
            --    'customer_id__ds_partitioned__extract_quarter',
            --    'customer_id__ds_partitioned__extract_month',
            --    'customer_id__ds_partitioned__extract_week',
            --    'customer_id__ds_partitioned__extract_day',
            --    'customer_id__ds_partitioned__extract_dow',
            --    'customer_id__ds_partitioned__extract_doy',
            --    'customer_id']
            SELECT
              subq_4.ds_partitioned__day
              , subq_4.ds_partitioned__week
              , subq_4.ds_partitioned__month
              , subq_4.ds_partitioned__quarter
              , subq_4.ds_partitioned__year
              , subq_4.ds_partitioned__extract_year
              , subq_4.ds_partitioned__extract_quarter
              , subq_4.ds_partitioned__extract_month
              , subq_4.ds_partitioned__extract_week
              , subq_4.ds_partitioned__extract_day
              , subq_4.ds_partitioned__extract_dow
              , subq_4.ds_partitioned__extract_doy
              , subq_4.customer_id__ds_partitioned__day
              , subq_4.customer_id__ds_partitioned__week
              , subq_4.customer_id__ds_partitioned__month
              , subq_4.customer_id__ds_partitioned__quarter
              , subq_4.customer_id__ds_partitioned__year
              , subq_4.customer_id__ds_partitioned__extract_year
              , subq_4.customer_id__ds_partitioned__extract_quarter
              , subq_4.customer_id__ds_partitioned__extract_month
              , subq_4.customer_id__ds_partitioned__extract_week
              , subq_4.customer_id__ds_partitioned__extract_day
              , subq_4.customer_id__ds_partitioned__extract_dow
              , subq_4.customer_id__ds_partitioned__extract_doy
              , subq_4.customer_id
              , subq_4.customer_name
              , subq_4.customer_atomic_weight
              , subq_4.customer_id__customer_name
              , subq_4.customer_id__customer_atomic_weight
            FROM (
              -- Read Elements From Semantic Model 'customer_table'
              SELECT
                customer_table_src_10013.customer_name
                , customer_table_src_10013.customer_atomic_weight
                , DATE_TRUNC(customer_table_src_10013.ds_partitioned, day) AS ds_partitioned__day
                , DATE_TRUNC(customer_table_src_10013.ds_partitioned, isoweek) AS ds_partitioned__week
                , DATE_TRUNC(customer_table_src_10013.ds_partitioned, month) AS ds_partitioned__month
                , DATE_TRUNC(customer_table_src_10013.ds_partitioned, quarter) AS ds_partitioned__quarter
                , DATE_TRUNC(customer_table_src_10013.ds_partitioned, year) AS ds_partitioned__year
                , EXTRACT(year FROM customer_table_src_10013.ds_partitioned) AS ds_partitioned__extract_year
                , EXTRACT(quarter FROM customer_table_src_10013.ds_partitioned) AS ds_partitioned__extract_quarter
                , EXTRACT(month FROM customer_table_src_10013.ds_partitioned) AS ds_partitioned__extract_month
                , EXTRACT(isoweek FROM customer_table_src_10013.ds_partitioned) AS ds_partitioned__extract_week
                , EXTRACT(day FROM customer_table_src_10013.ds_partitioned) AS ds_partitioned__extract_day
                , EXTRACT(dayofweek FROM customer_table_src_10013.ds_partitioned) AS ds_partitioned__extract_dow
                , EXTRACT(dayofyear FROM customer_table_src_10013.ds_partitioned) AS ds_partitioned__extract_doy
                , customer_table_src_10013.customer_name AS customer_id__customer_name
                , customer_table_src_10013.customer_atomic_weight AS customer_id__customer_atomic_weight
                , DATE_TRUNC(customer_table_src_10013.ds_partitioned, day) AS customer_id__ds_partitioned__day
                , DATE_TRUNC(customer_table_src_10013.ds_partitioned, isoweek) AS customer_id__ds_partitioned__week
                , DATE_TRUNC(customer_table_src_10013.ds_partitioned, month) AS customer_id__ds_partitioned__month
                , DATE_TRUNC(customer_table_src_10013.ds_partitioned, quarter) AS customer_id__ds_partitioned__quarter
                , DATE_TRUNC(customer_table_src_10013.ds_partitioned, year) AS customer_id__ds_partitioned__year
                , EXTRACT(year FROM customer_table_src_10013.ds_partitioned) AS customer_id__ds_partitioned__extract_year
                , EXTRACT(quarter FROM customer_table_src_10013.ds_partitioned) AS customer_id__ds_partitioned__extract_quarter
                , EXTRACT(month FROM customer_table_src_10013.ds_partitioned) AS customer_id__ds_partitioned__extract_month
                , EXTRACT(isoweek FROM customer_table_src_10013.ds_partitioned) AS customer_id__ds_partitioned__extract_week
                , EXTRACT(day FROM customer_table_src_10013.ds_partitioned) AS customer_id__ds_partitioned__extract_day
                , EXTRACT(dayofweek FROM customer_table_src_10013.ds_partitioned) AS customer_id__ds_partitioned__extract_dow
                , EXTRACT(dayofyear FROM customer_table_src_10013.ds_partitioned) AS customer_id__ds_partitioned__extract_doy
                , customer_table_src_10013.customer_id
              FROM ***************************.customer_table customer_table_src_10013
            ) subq_4
          ) subq_5
          ON
            (
              subq_3.customer_id = subq_5.customer_id
            ) AND (
              subq_3.ds_partitioned__day = subq_5.ds_partitioned__day
            )
        ) subq_6
      ) subq_7
      ON
        (
          subq_2.account_id = subq_7.account_id
        ) AND (
          subq_2.ds_partitioned__day = subq_7.ds_partitioned__day
        )
    ) subq_8
  ) subq_9
  GROUP BY
    account_id__customer_id__customer_name
) subq_10
