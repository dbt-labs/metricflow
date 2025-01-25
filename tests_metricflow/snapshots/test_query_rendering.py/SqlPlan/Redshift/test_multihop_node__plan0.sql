test_name: test_multihop_node
test_filename: test_query_rendering.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 2 dimensions.
sql_engine: Redshift
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_11.account_id__customer_id__customer_name
  , nr_subq_11.txn_count
FROM (
  -- Aggregate Measures
  SELECT
    nr_subq_10.account_id__customer_id__customer_name
    , SUM(nr_subq_10.txn_count) AS txn_count
  FROM (
    -- Pass Only Elements: ['txn_count', 'account_id__customer_id__customer_name']
    SELECT
      nr_subq_9.account_id__customer_id__customer_name
      , nr_subq_9.txn_count
    FROM (
      -- Join Standard Outputs
      SELECT
        nr_subq_8.customer_id__customer_name AS account_id__customer_id__customer_name
        , nr_subq_8.ds_partitioned__day AS account_id__ds_partitioned__day
        , nr_subq_3.ds_partitioned__day AS ds_partitioned__day
        , nr_subq_3.ds_partitioned__week AS ds_partitioned__week
        , nr_subq_3.ds_partitioned__month AS ds_partitioned__month
        , nr_subq_3.ds_partitioned__quarter AS ds_partitioned__quarter
        , nr_subq_3.ds_partitioned__year AS ds_partitioned__year
        , nr_subq_3.ds_partitioned__extract_year AS ds_partitioned__extract_year
        , nr_subq_3.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
        , nr_subq_3.ds_partitioned__extract_month AS ds_partitioned__extract_month
        , nr_subq_3.ds_partitioned__extract_day AS ds_partitioned__extract_day
        , nr_subq_3.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
        , nr_subq_3.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
        , nr_subq_3.ds__day AS ds__day
        , nr_subq_3.ds__week AS ds__week
        , nr_subq_3.ds__month AS ds__month
        , nr_subq_3.ds__quarter AS ds__quarter
        , nr_subq_3.ds__year AS ds__year
        , nr_subq_3.ds__extract_year AS ds__extract_year
        , nr_subq_3.ds__extract_quarter AS ds__extract_quarter
        , nr_subq_3.ds__extract_month AS ds__extract_month
        , nr_subq_3.ds__extract_day AS ds__extract_day
        , nr_subq_3.ds__extract_dow AS ds__extract_dow
        , nr_subq_3.ds__extract_doy AS ds__extract_doy
        , nr_subq_3.account_id__ds_partitioned__day AS account_id__ds_partitioned__day
        , nr_subq_3.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
        , nr_subq_3.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
        , nr_subq_3.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
        , nr_subq_3.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
        , nr_subq_3.account_id__ds_partitioned__extract_year AS account_id__ds_partitioned__extract_year
        , nr_subq_3.account_id__ds_partitioned__extract_quarter AS account_id__ds_partitioned__extract_quarter
        , nr_subq_3.account_id__ds_partitioned__extract_month AS account_id__ds_partitioned__extract_month
        , nr_subq_3.account_id__ds_partitioned__extract_day AS account_id__ds_partitioned__extract_day
        , nr_subq_3.account_id__ds_partitioned__extract_dow AS account_id__ds_partitioned__extract_dow
        , nr_subq_3.account_id__ds_partitioned__extract_doy AS account_id__ds_partitioned__extract_doy
        , nr_subq_3.account_id__ds__day AS account_id__ds__day
        , nr_subq_3.account_id__ds__week AS account_id__ds__week
        , nr_subq_3.account_id__ds__month AS account_id__ds__month
        , nr_subq_3.account_id__ds__quarter AS account_id__ds__quarter
        , nr_subq_3.account_id__ds__year AS account_id__ds__year
        , nr_subq_3.account_id__ds__extract_year AS account_id__ds__extract_year
        , nr_subq_3.account_id__ds__extract_quarter AS account_id__ds__extract_quarter
        , nr_subq_3.account_id__ds__extract_month AS account_id__ds__extract_month
        , nr_subq_3.account_id__ds__extract_day AS account_id__ds__extract_day
        , nr_subq_3.account_id__ds__extract_dow AS account_id__ds__extract_dow
        , nr_subq_3.account_id__ds__extract_doy AS account_id__ds__extract_doy
        , nr_subq_3.metric_time__day AS metric_time__day
        , nr_subq_3.metric_time__week AS metric_time__week
        , nr_subq_3.metric_time__month AS metric_time__month
        , nr_subq_3.metric_time__quarter AS metric_time__quarter
        , nr_subq_3.metric_time__year AS metric_time__year
        , nr_subq_3.metric_time__extract_year AS metric_time__extract_year
        , nr_subq_3.metric_time__extract_quarter AS metric_time__extract_quarter
        , nr_subq_3.metric_time__extract_month AS metric_time__extract_month
        , nr_subq_3.metric_time__extract_day AS metric_time__extract_day
        , nr_subq_3.metric_time__extract_dow AS metric_time__extract_dow
        , nr_subq_3.metric_time__extract_doy AS metric_time__extract_doy
        , nr_subq_3.account_id AS account_id
        , nr_subq_3.account_month AS account_month
        , nr_subq_3.account_id__account_month AS account_id__account_month
        , nr_subq_3.txn_count AS txn_count
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
            , DATE_TRUNC('day', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__day
            , DATE_TRUNC('week', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__week
            , DATE_TRUNC('month', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__month
            , DATE_TRUNC('quarter', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__quarter
            , DATE_TRUNC('year', account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__year
            , EXTRACT(year FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_year
            , EXTRACT(quarter FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_quarter
            , EXTRACT(month FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_month
            , EXTRACT(day FROM account_month_txns_src_22000.ds_partitioned) AS ds_partitioned__extract_day
            , CASE WHEN EXTRACT(dow FROM account_month_txns_src_22000.ds_partitioned) = 0 THEN EXTRACT(dow FROM account_month_txns_src_22000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM account_month_txns_src_22000.ds_partitioned) END AS ds_partitioned__extract_dow
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
            , CASE WHEN EXTRACT(dow FROM account_month_txns_src_22000.ds) = 0 THEN EXTRACT(dow FROM account_month_txns_src_22000.ds) + 7 ELSE EXTRACT(dow FROM account_month_txns_src_22000.ds) END AS ds__extract_dow
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
            , CASE WHEN EXTRACT(dow FROM account_month_txns_src_22000.ds_partitioned) = 0 THEN EXTRACT(dow FROM account_month_txns_src_22000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM account_month_txns_src_22000.ds_partitioned) END AS account_id__ds_partitioned__extract_dow
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
            , CASE WHEN EXTRACT(dow FROM account_month_txns_src_22000.ds) = 0 THEN EXTRACT(dow FROM account_month_txns_src_22000.ds) + 7 ELSE EXTRACT(dow FROM account_month_txns_src_22000.ds) END AS account_id__ds__extract_dow
            , EXTRACT(doy FROM account_month_txns_src_22000.ds) AS account_id__ds__extract_doy
            , account_month_txns_src_22000.account_month AS account_id__account_month
            , account_month_txns_src_22000.account_id
          FROM ***************************.account_month_txns account_month_txns_src_22000
        ) nr_subq_22000
      ) nr_subq_3
      LEFT OUTER JOIN (
        -- Pass Only Elements: ['customer_id__customer_name', 'ds_partitioned__day', 'account_id']
        SELECT
          nr_subq_7.ds_partitioned__day
          , nr_subq_7.account_id
          , nr_subq_7.customer_id__customer_name
        FROM (
          -- Join Standard Outputs
          SELECT
            nr_subq_6.customer_name AS customer_id__customer_name
            , nr_subq_6.customer_atomic_weight AS customer_id__customer_atomic_weight
            , nr_subq_6.ds_partitioned__day AS customer_id__ds_partitioned__day
            , nr_subq_6.ds_partitioned__week AS customer_id__ds_partitioned__week
            , nr_subq_6.ds_partitioned__month AS customer_id__ds_partitioned__month
            , nr_subq_6.ds_partitioned__quarter AS customer_id__ds_partitioned__quarter
            , nr_subq_6.ds_partitioned__year AS customer_id__ds_partitioned__year
            , nr_subq_6.ds_partitioned__extract_year AS customer_id__ds_partitioned__extract_year
            , nr_subq_6.ds_partitioned__extract_quarter AS customer_id__ds_partitioned__extract_quarter
            , nr_subq_6.ds_partitioned__extract_month AS customer_id__ds_partitioned__extract_month
            , nr_subq_6.ds_partitioned__extract_day AS customer_id__ds_partitioned__extract_day
            , nr_subq_6.ds_partitioned__extract_dow AS customer_id__ds_partitioned__extract_dow
            , nr_subq_6.ds_partitioned__extract_doy AS customer_id__ds_partitioned__extract_doy
            , nr_subq_6.metric_time__day AS customer_id__metric_time__day
            , nr_subq_6.metric_time__week AS customer_id__metric_time__week
            , nr_subq_6.metric_time__month AS customer_id__metric_time__month
            , nr_subq_6.metric_time__quarter AS customer_id__metric_time__quarter
            , nr_subq_6.metric_time__year AS customer_id__metric_time__year
            , nr_subq_6.metric_time__extract_year AS customer_id__metric_time__extract_year
            , nr_subq_6.metric_time__extract_quarter AS customer_id__metric_time__extract_quarter
            , nr_subq_6.metric_time__extract_month AS customer_id__metric_time__extract_month
            , nr_subq_6.metric_time__extract_day AS customer_id__metric_time__extract_day
            , nr_subq_6.metric_time__extract_dow AS customer_id__metric_time__extract_dow
            , nr_subq_6.metric_time__extract_doy AS customer_id__metric_time__extract_doy
            , nr_subq_4.ds_partitioned__day AS ds_partitioned__day
            , nr_subq_4.ds_partitioned__week AS ds_partitioned__week
            , nr_subq_4.ds_partitioned__month AS ds_partitioned__month
            , nr_subq_4.ds_partitioned__quarter AS ds_partitioned__quarter
            , nr_subq_4.ds_partitioned__year AS ds_partitioned__year
            , nr_subq_4.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , nr_subq_4.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , nr_subq_4.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , nr_subq_4.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , nr_subq_4.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , nr_subq_4.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , nr_subq_4.account_id__ds_partitioned__day AS account_id__ds_partitioned__day
            , nr_subq_4.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
            , nr_subq_4.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
            , nr_subq_4.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
            , nr_subq_4.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
            , nr_subq_4.account_id__ds_partitioned__extract_year AS account_id__ds_partitioned__extract_year
            , nr_subq_4.account_id__ds_partitioned__extract_quarter AS account_id__ds_partitioned__extract_quarter
            , nr_subq_4.account_id__ds_partitioned__extract_month AS account_id__ds_partitioned__extract_month
            , nr_subq_4.account_id__ds_partitioned__extract_day AS account_id__ds_partitioned__extract_day
            , nr_subq_4.account_id__ds_partitioned__extract_dow AS account_id__ds_partitioned__extract_dow
            , nr_subq_4.account_id__ds_partitioned__extract_doy AS account_id__ds_partitioned__extract_doy
            , nr_subq_4.bridge_account__ds_partitioned__day AS bridge_account__ds_partitioned__day
            , nr_subq_4.bridge_account__ds_partitioned__week AS bridge_account__ds_partitioned__week
            , nr_subq_4.bridge_account__ds_partitioned__month AS bridge_account__ds_partitioned__month
            , nr_subq_4.bridge_account__ds_partitioned__quarter AS bridge_account__ds_partitioned__quarter
            , nr_subq_4.bridge_account__ds_partitioned__year AS bridge_account__ds_partitioned__year
            , nr_subq_4.bridge_account__ds_partitioned__extract_year AS bridge_account__ds_partitioned__extract_year
            , nr_subq_4.bridge_account__ds_partitioned__extract_quarter AS bridge_account__ds_partitioned__extract_quarter
            , nr_subq_4.bridge_account__ds_partitioned__extract_month AS bridge_account__ds_partitioned__extract_month
            , nr_subq_4.bridge_account__ds_partitioned__extract_day AS bridge_account__ds_partitioned__extract_day
            , nr_subq_4.bridge_account__ds_partitioned__extract_dow AS bridge_account__ds_partitioned__extract_dow
            , nr_subq_4.bridge_account__ds_partitioned__extract_doy AS bridge_account__ds_partitioned__extract_doy
            , nr_subq_4.metric_time__day AS metric_time__day
            , nr_subq_4.metric_time__week AS metric_time__week
            , nr_subq_4.metric_time__month AS metric_time__month
            , nr_subq_4.metric_time__quarter AS metric_time__quarter
            , nr_subq_4.metric_time__year AS metric_time__year
            , nr_subq_4.metric_time__extract_year AS metric_time__extract_year
            , nr_subq_4.metric_time__extract_quarter AS metric_time__extract_quarter
            , nr_subq_4.metric_time__extract_month AS metric_time__extract_month
            , nr_subq_4.metric_time__extract_day AS metric_time__extract_day
            , nr_subq_4.metric_time__extract_dow AS metric_time__extract_dow
            , nr_subq_4.metric_time__extract_doy AS metric_time__extract_doy
            , nr_subq_4.account_id AS account_id
            , nr_subq_4.customer_id AS customer_id
            , nr_subq_4.account_id__customer_id AS account_id__customer_id
            , nr_subq_4.bridge_account__account_id AS bridge_account__account_id
            , nr_subq_4.bridge_account__customer_id AS bridge_account__customer_id
            , nr_subq_4.extra_dim AS extra_dim
            , nr_subq_4.account_id__extra_dim AS account_id__extra_dim
            , nr_subq_4.bridge_account__extra_dim AS bridge_account__extra_dim
            , nr_subq_4.account_customer_combos AS account_customer_combos
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
                , DATE_TRUNC('day', bridge_table_src_22000.ds_partitioned) AS ds_partitioned__day
                , DATE_TRUNC('week', bridge_table_src_22000.ds_partitioned) AS ds_partitioned__week
                , DATE_TRUNC('month', bridge_table_src_22000.ds_partitioned) AS ds_partitioned__month
                , DATE_TRUNC('quarter', bridge_table_src_22000.ds_partitioned) AS ds_partitioned__quarter
                , DATE_TRUNC('year', bridge_table_src_22000.ds_partitioned) AS ds_partitioned__year
                , EXTRACT(year FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_year
                , EXTRACT(quarter FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_quarter
                , EXTRACT(month FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_month
                , EXTRACT(day FROM bridge_table_src_22000.ds_partitioned) AS ds_partitioned__extract_day
                , CASE WHEN EXTRACT(dow FROM bridge_table_src_22000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bridge_table_src_22000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bridge_table_src_22000.ds_partitioned) END AS ds_partitioned__extract_dow
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
                , CASE WHEN EXTRACT(dow FROM bridge_table_src_22000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bridge_table_src_22000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bridge_table_src_22000.ds_partitioned) END AS account_id__ds_partitioned__extract_dow
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
                , CASE WHEN EXTRACT(dow FROM bridge_table_src_22000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bridge_table_src_22000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bridge_table_src_22000.ds_partitioned) END AS bridge_account__ds_partitioned__extract_dow
                , EXTRACT(doy FROM bridge_table_src_22000.ds_partitioned) AS bridge_account__ds_partitioned__extract_doy
                , bridge_table_src_22000.account_id
                , bridge_table_src_22000.customer_id
                , bridge_table_src_22000.customer_id AS account_id__customer_id
                , bridge_table_src_22000.account_id AS bridge_account__account_id
                , bridge_table_src_22000.customer_id AS bridge_account__customer_id
              FROM ***************************.bridge_table bridge_table_src_22000
            ) nr_subq_22001
          ) nr_subq_4
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
              nr_subq_5.ds_partitioned__day
              , nr_subq_5.ds_partitioned__week
              , nr_subq_5.ds_partitioned__month
              , nr_subq_5.ds_partitioned__quarter
              , nr_subq_5.ds_partitioned__year
              , nr_subq_5.ds_partitioned__extract_year
              , nr_subq_5.ds_partitioned__extract_quarter
              , nr_subq_5.ds_partitioned__extract_month
              , nr_subq_5.ds_partitioned__extract_day
              , nr_subq_5.ds_partitioned__extract_dow
              , nr_subq_5.ds_partitioned__extract_doy
              , nr_subq_5.customer_id__ds_partitioned__day
              , nr_subq_5.customer_id__ds_partitioned__week
              , nr_subq_5.customer_id__ds_partitioned__month
              , nr_subq_5.customer_id__ds_partitioned__quarter
              , nr_subq_5.customer_id__ds_partitioned__year
              , nr_subq_5.customer_id__ds_partitioned__extract_year
              , nr_subq_5.customer_id__ds_partitioned__extract_quarter
              , nr_subq_5.customer_id__ds_partitioned__extract_month
              , nr_subq_5.customer_id__ds_partitioned__extract_day
              , nr_subq_5.customer_id__ds_partitioned__extract_dow
              , nr_subq_5.customer_id__ds_partitioned__extract_doy
              , nr_subq_5.metric_time__day
              , nr_subq_5.metric_time__week
              , nr_subq_5.metric_time__month
              , nr_subq_5.metric_time__quarter
              , nr_subq_5.metric_time__year
              , nr_subq_5.metric_time__extract_year
              , nr_subq_5.metric_time__extract_quarter
              , nr_subq_5.metric_time__extract_month
              , nr_subq_5.metric_time__extract_day
              , nr_subq_5.metric_time__extract_dow
              , nr_subq_5.metric_time__extract_doy
              , nr_subq_5.customer_id
              , nr_subq_5.customer_name
              , nr_subq_5.customer_atomic_weight
              , nr_subq_5.customer_id__customer_name
              , nr_subq_5.customer_id__customer_atomic_weight
            FROM (
              -- Metric Time Dimension 'ds_partitioned'
              SELECT
                nr_subq_22003.ds_partitioned__day
                , nr_subq_22003.ds_partitioned__week
                , nr_subq_22003.ds_partitioned__month
                , nr_subq_22003.ds_partitioned__quarter
                , nr_subq_22003.ds_partitioned__year
                , nr_subq_22003.ds_partitioned__extract_year
                , nr_subq_22003.ds_partitioned__extract_quarter
                , nr_subq_22003.ds_partitioned__extract_month
                , nr_subq_22003.ds_partitioned__extract_day
                , nr_subq_22003.ds_partitioned__extract_dow
                , nr_subq_22003.ds_partitioned__extract_doy
                , nr_subq_22003.customer_id__ds_partitioned__day
                , nr_subq_22003.customer_id__ds_partitioned__week
                , nr_subq_22003.customer_id__ds_partitioned__month
                , nr_subq_22003.customer_id__ds_partitioned__quarter
                , nr_subq_22003.customer_id__ds_partitioned__year
                , nr_subq_22003.customer_id__ds_partitioned__extract_year
                , nr_subq_22003.customer_id__ds_partitioned__extract_quarter
                , nr_subq_22003.customer_id__ds_partitioned__extract_month
                , nr_subq_22003.customer_id__ds_partitioned__extract_day
                , nr_subq_22003.customer_id__ds_partitioned__extract_dow
                , nr_subq_22003.customer_id__ds_partitioned__extract_doy
                , nr_subq_22003.ds_partitioned__day AS metric_time__day
                , nr_subq_22003.ds_partitioned__week AS metric_time__week
                , nr_subq_22003.ds_partitioned__month AS metric_time__month
                , nr_subq_22003.ds_partitioned__quarter AS metric_time__quarter
                , nr_subq_22003.ds_partitioned__year AS metric_time__year
                , nr_subq_22003.ds_partitioned__extract_year AS metric_time__extract_year
                , nr_subq_22003.ds_partitioned__extract_quarter AS metric_time__extract_quarter
                , nr_subq_22003.ds_partitioned__extract_month AS metric_time__extract_month
                , nr_subq_22003.ds_partitioned__extract_day AS metric_time__extract_day
                , nr_subq_22003.ds_partitioned__extract_dow AS metric_time__extract_dow
                , nr_subq_22003.ds_partitioned__extract_doy AS metric_time__extract_doy
                , nr_subq_22003.customer_id
                , nr_subq_22003.customer_name
                , nr_subq_22003.customer_atomic_weight
                , nr_subq_22003.customer_id__customer_name
                , nr_subq_22003.customer_id__customer_atomic_weight
                , nr_subq_22003.customers
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
                  , CASE WHEN EXTRACT(dow FROM customer_table_src_22000.ds_partitioned) = 0 THEN EXTRACT(dow FROM customer_table_src_22000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM customer_table_src_22000.ds_partitioned) END AS ds_partitioned__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM customer_table_src_22000.ds_partitioned) = 0 THEN EXTRACT(dow FROM customer_table_src_22000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM customer_table_src_22000.ds_partitioned) END AS customer_id__ds_partitioned__extract_dow
                  , EXTRACT(doy FROM customer_table_src_22000.ds_partitioned) AS customer_id__ds_partitioned__extract_doy
                  , customer_table_src_22000.customer_id
                FROM ***************************.customer_table customer_table_src_22000
              ) nr_subq_22003
            ) nr_subq_5
          ) nr_subq_6
          ON
            (
              nr_subq_4.customer_id = nr_subq_6.customer_id
            ) AND (
              nr_subq_4.ds_partitioned__day = nr_subq_6.ds_partitioned__day
            )
        ) nr_subq_7
      ) nr_subq_8
      ON
        (
          nr_subq_3.account_id = nr_subq_8.account_id
        ) AND (
          nr_subq_3.ds_partitioned__day = nr_subq_8.ds_partitioned__day
        )
    ) nr_subq_9
  ) nr_subq_10
  GROUP BY
    nr_subq_10.account_id__customer_id__customer_name
) nr_subq_11
