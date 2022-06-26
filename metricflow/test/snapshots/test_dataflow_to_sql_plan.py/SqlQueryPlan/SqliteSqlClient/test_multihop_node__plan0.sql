-- Compute Metrics via Expressions
SELECT
  subq_10.txn_count
  , subq_10.account_id__customer_id__customer_name
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_9.txn_count) AS txn_count
    , subq_9.account_id__customer_id__customer_name
  FROM (
    -- Pass Only Elements:
    --   ['txn_count', 'account_id__customer_id__customer_name']
    SELECT
      subq_8.txn_count
      , subq_8.account_id__customer_id__customer_name
    FROM (
      -- Join Standard Outputs
      SELECT
        subq_2.txn_count AS txn_count
        , subq_7.customer_id__customer_name AS account_id__customer_id__customer_name
        , subq_2.ds_partitioned AS ds_partitioned
        , subq_7.ds_partitioned AS account_id__ds_partitioned
        , subq_2.account_id AS account_id
      FROM (
        -- Pass Only Elements:
        --   ['txn_count', 'account_id', 'ds_partitioned']
        SELECT
          subq_1.txn_count
          , subq_1.ds_partitioned
          , subq_1.account_id
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_0.txn_count
            , subq_0.account_month
            , subq_0.account_id__account_month
            , subq_0.ds_partitioned
            , subq_0.ds_partitioned__week
            , subq_0.ds_partitioned__month
            , subq_0.ds_partitioned__quarter
            , subq_0.ds_partitioned__year
            , subq_0.ds
            , subq_0.ds__week
            , subq_0.ds__month
            , subq_0.ds__quarter
            , subq_0.ds__year
            , subq_0.account_id__ds_partitioned
            , subq_0.account_id__ds_partitioned__week
            , subq_0.account_id__ds_partitioned__month
            , subq_0.account_id__ds_partitioned__quarter
            , subq_0.account_id__ds_partitioned__year
            , subq_0.account_id__ds
            , subq_0.account_id__ds__week
            , subq_0.account_id__ds__month
            , subq_0.account_id__ds__quarter
            , subq_0.account_id__ds__year
            , subq_0.ds AS metric_time
            , subq_0.ds__week AS metric_time__week
            , subq_0.ds__month AS metric_time__month
            , subq_0.ds__quarter AS metric_time__quarter
            , subq_0.ds__year AS metric_time__year
            , subq_0.account_id
          FROM (
            -- Read Elements From Data Source 'account_month_txns'
            SELECT
              account_month_txns_src_10009.txn_count
              , account_month_txns_src_10009.ds_partitioned
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__week
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__month
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__quarter
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__year
              , account_month_txns_src_10009.ds
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
              , account_month_txns_src_10009.account_month
              , account_month_txns_src_10009.ds_partitioned AS account_id__ds_partitioned
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__week
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__month
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__quarter
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__year
              , account_month_txns_src_10009.ds AS account_id__ds
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds__week
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds__month
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds__quarter
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds__year
              , account_month_txns_src_10009.account_month AS account_id__account_month
              , account_month_txns_src_10009.account_id
            FROM ***************************.account_month_txns account_month_txns_src_10009
          ) subq_0
        ) subq_1
      ) subq_2
      LEFT OUTER JOIN (
        -- Pass Only Elements:
        --   ['account_id', 'ds_partitioned', 'customer_id__customer_name']
        SELECT
          subq_6.customer_id__customer_name
          , subq_6.ds_partitioned
          , subq_6.account_id
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_3.extra_dim AS extra_dim
            , subq_3.account_id__extra_dim AS account_id__extra_dim
            , subq_5.customer_name AS customer_id__customer_name
            , subq_5.customer_atomic_weight AS customer_id__customer_atomic_weight
            , subq_3.ds_partitioned AS ds_partitioned
            , subq_3.ds_partitioned__week AS ds_partitioned__week
            , subq_3.ds_partitioned__month AS ds_partitioned__month
            , subq_3.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_3.ds_partitioned__year AS ds_partitioned__year
            , subq_3.account_id__ds_partitioned AS account_id__ds_partitioned
            , subq_3.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
            , subq_3.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
            , subq_3.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
            , subq_3.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
            , subq_5.ds_partitioned AS customer_id__ds_partitioned
            , subq_5.ds_partitioned__week AS customer_id__ds_partitioned__week
            , subq_5.ds_partitioned__month AS customer_id__ds_partitioned__month
            , subq_5.ds_partitioned__quarter AS customer_id__ds_partitioned__quarter
            , subq_5.ds_partitioned__year AS customer_id__ds_partitioned__year
            , subq_3.account_id AS account_id
            , subq_3.customer_id AS customer_id
            , subq_3.account_id__customer_id AS account_id__customer_id
          FROM (
            -- Read Elements From Data Source 'bridge_table'
            SELECT
              bridge_table_src_10010.extra_dim
              , bridge_table_src_10010.ds_partitioned
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__week
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__month
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__quarter
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__year
              , bridge_table_src_10010.extra_dim AS account_id__extra_dim
              , bridge_table_src_10010.ds_partitioned AS account_id__ds_partitioned
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__week
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__month
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__quarter
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__year
              , bridge_table_src_10010.account_id
              , bridge_table_src_10010.customer_id
              , bridge_table_src_10010.customer_id AS account_id__customer_id
            FROM ***************************.bridge_table bridge_table_src_10010
          ) subq_3
          LEFT OUTER JOIN (
            -- Pass Only Elements:
            --   ['customer_name',
            --    'customer_atomic_weight',
            --    'customer_id__customer_name',
            --    'customer_id__customer_atomic_weight',
            --    'customer_id',
            --    'ds_partitioned',
            --    'ds_partitioned__week',
            --    'ds_partitioned__month',
            --    'ds_partitioned__quarter',
            --    'ds_partitioned__year',
            --    'customer_id__ds_partitioned',
            --    'customer_id__ds_partitioned__week',
            --    'customer_id__ds_partitioned__month',
            --    'customer_id__ds_partitioned__quarter',
            --    'customer_id__ds_partitioned__year']
            SELECT
              subq_4.customer_name
              , subq_4.customer_atomic_weight
              , subq_4.customer_id__customer_name
              , subq_4.customer_id__customer_atomic_weight
              , subq_4.ds_partitioned
              , subq_4.ds_partitioned__week
              , subq_4.ds_partitioned__month
              , subq_4.ds_partitioned__quarter
              , subq_4.ds_partitioned__year
              , subq_4.customer_id__ds_partitioned
              , subq_4.customer_id__ds_partitioned__week
              , subq_4.customer_id__ds_partitioned__month
              , subq_4.customer_id__ds_partitioned__quarter
              , subq_4.customer_id__ds_partitioned__year
              , subq_4.customer_id
            FROM (
              -- Read Elements From Data Source 'customer_table'
              SELECT
                customer_table_src_10012.customer_name
                , customer_table_src_10012.customer_atomic_weight
                , customer_table_src_10012.ds_partitioned
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__week
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__month
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__quarter
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__year
                , customer_table_src_10012.customer_name AS customer_id__customer_name
                , customer_table_src_10012.customer_atomic_weight AS customer_id__customer_atomic_weight
                , customer_table_src_10012.ds_partitioned AS customer_id__ds_partitioned
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS customer_id__ds_partitioned__week
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS customer_id__ds_partitioned__month
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS customer_id__ds_partitioned__quarter
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS customer_id__ds_partitioned__year
                , customer_table_src_10012.customer_id
              FROM ***************************.customer_table customer_table_src_10012
            ) subq_4
          ) subq_5
          ON
            (
              subq_3.customer_id = subq_5.customer_id
            ) AND (
              subq_3.ds_partitioned = subq_5.ds_partitioned
            )
        ) subq_6
      ) subq_7
      ON
        (
          subq_2.account_id = subq_7.account_id
        ) AND (
          subq_2.ds_partitioned = subq_7.ds_partitioned
        )
    ) subq_8
  ) subq_9
  GROUP BY
    subq_9.account_id__customer_id__customer_name
) subq_10
