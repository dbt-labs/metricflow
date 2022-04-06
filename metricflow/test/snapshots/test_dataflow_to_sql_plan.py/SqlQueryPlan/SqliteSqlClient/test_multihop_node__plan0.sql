-- Compute Metrics via Expressions
SELECT
  subq_9.txn_count
  , subq_9.account_id__customer_id__customer_name
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_8.txn_count) AS txn_count
    , subq_8.account_id__customer_id__customer_name
  FROM (
    -- Pass Only Elements:
    --   ['txn_count', 'account_id__customer_id__customer_name']
    SELECT
      subq_7.txn_count
      , subq_7.account_id__customer_id__customer_name
    FROM (
      -- Join Standard Outputs
      SELECT
        subq_1.txn_count AS txn_count
        , subq_6.customer_id__customer_name AS account_id__customer_id__customer_name
        , subq_1.ds_partitioned AS ds_partitioned
        , subq_6.ds_partitioned AS account_id__ds_partitioned
        , subq_1.account_id AS account_id
      FROM (
        -- Pass Only Elements:
        --   ['txn_count', 'account_id', 'ds_partitioned']
        SELECT
          subq_0.txn_count
          , subq_0.ds_partitioned
          , subq_0.account_id
        FROM (
          -- Read Elements From Data Source 'account_month_txns'
          SELECT
            account_month_txns_src_0.txn_count
            , account_month_txns_src_0.ds_partitioned
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__week
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__month
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__quarter
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__year
            , account_month_txns_src_0.ds
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
            , account_month_txns_src_0.account_month
            , account_month_txns_src_0.ds_partitioned AS account_id__ds_partitioned
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__week
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__month
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__quarter
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__year
            , account_month_txns_src_0.ds AS account_id__ds
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds__week
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds__month
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds__quarter
            , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds__year
            , account_month_txns_src_0.account_month AS account_id__account_month
            , account_month_txns_src_0.account_id
          FROM ***************************.account_month_txns account_month_txns_src_0
        ) subq_0
      ) subq_1
      LEFT OUTER JOIN (
        -- Pass Only Elements:
        --   ['account_id', 'ds_partitioned', 'customer_id__customer_name']
        SELECT
          subq_5.customer_id__customer_name
          , subq_5.ds_partitioned
          , subq_5.account_id
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_2.extra_dim AS extra_dim
            , subq_2.account_id__extra_dim AS account_id__extra_dim
            , subq_4.customer_name AS customer_id__customer_name
            , subq_4.customer_atomic_weight AS customer_id__customer_atomic_weight
            , subq_2.ds_partitioned AS ds_partitioned
            , subq_2.ds_partitioned__week AS ds_partitioned__week
            , subq_2.ds_partitioned__month AS ds_partitioned__month
            , subq_2.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_2.ds_partitioned__year AS ds_partitioned__year
            , subq_2.account_id__ds_partitioned AS account_id__ds_partitioned
            , subq_2.account_id__ds_partitioned__week AS account_id__ds_partitioned__week
            , subq_2.account_id__ds_partitioned__month AS account_id__ds_partitioned__month
            , subq_2.account_id__ds_partitioned__quarter AS account_id__ds_partitioned__quarter
            , subq_2.account_id__ds_partitioned__year AS account_id__ds_partitioned__year
            , subq_4.ds_partitioned AS customer_id__ds_partitioned
            , subq_4.ds_partitioned__week AS customer_id__ds_partitioned__week
            , subq_4.ds_partitioned__month AS customer_id__ds_partitioned__month
            , subq_4.ds_partitioned__quarter AS customer_id__ds_partitioned__quarter
            , subq_4.ds_partitioned__year AS customer_id__ds_partitioned__year
            , subq_2.account_id AS account_id
            , subq_2.customer_id AS customer_id
            , subq_2.account_id__customer_id AS account_id__customer_id
          FROM (
            -- Read Elements From Data Source 'bridge_table'
            SELECT
              bridge_table_src_1.extra_dim
              , bridge_table_src_1.ds_partitioned
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__week
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__month
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__quarter
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__year
              , bridge_table_src_1.extra_dim AS account_id__extra_dim
              , bridge_table_src_1.ds_partitioned AS account_id__ds_partitioned
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__week
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__month
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__quarter
              , '__DATE_TRUNC_NOT_SUPPORTED__' AS account_id__ds_partitioned__year
              , bridge_table_src_1.account_id
              , bridge_table_src_1.customer_id
              , bridge_table_src_1.customer_id AS account_id__customer_id
            FROM ***************************.bridge_table bridge_table_src_1
          ) subq_2
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
              subq_3.customer_name
              , subq_3.customer_atomic_weight
              , subq_3.customer_id__customer_name
              , subq_3.customer_id__customer_atomic_weight
              , subq_3.ds_partitioned
              , subq_3.ds_partitioned__week
              , subq_3.ds_partitioned__month
              , subq_3.ds_partitioned__quarter
              , subq_3.ds_partitioned__year
              , subq_3.customer_id__ds_partitioned
              , subq_3.customer_id__ds_partitioned__week
              , subq_3.customer_id__ds_partitioned__month
              , subq_3.customer_id__ds_partitioned__quarter
              , subq_3.customer_id__ds_partitioned__year
              , subq_3.customer_id
            FROM (
              -- Read Elements From Data Source 'customer_table'
              SELECT
                customer_table_src_3.customer_name
                , customer_table_src_3.customer_atomic_weight
                , customer_table_src_3.ds_partitioned
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__week
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__month
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__quarter
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__year
                , customer_table_src_3.customer_name AS customer_id__customer_name
                , customer_table_src_3.customer_atomic_weight AS customer_id__customer_atomic_weight
                , customer_table_src_3.ds_partitioned AS customer_id__ds_partitioned
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS customer_id__ds_partitioned__week
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS customer_id__ds_partitioned__month
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS customer_id__ds_partitioned__quarter
                , '__DATE_TRUNC_NOT_SUPPORTED__' AS customer_id__ds_partitioned__year
                , customer_table_src_3.customer_id
              FROM ***************************.customer_table customer_table_src_3
            ) subq_3
          ) subq_4
          ON
            (
              subq_2.customer_id = subq_4.customer_id
            ) AND (
              subq_2.ds_partitioned = subq_4.ds_partitioned
            )
        ) subq_5
      ) subq_6
      ON
        (
          subq_1.account_id = subq_6.account_id
        ) AND (
          subq_1.ds_partitioned = subq_6.ds_partitioned
        )
    ) subq_7
  ) subq_8
  GROUP BY
    subq_8.account_id__customer_id__customer_name
) subq_9
