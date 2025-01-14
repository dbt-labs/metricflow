test_name: test_inner_query_multi_hop
test_filename: test_metric_filter_rendering.py
docstring:
  Tests rendering for a metric filter using a two-hop join in the inner query.
sql_engine: Clickhouse
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['third_hop_count',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  COUNT(DISTINCT third_hop_count) AS third_hop_count
FROM (
  -- Join Standard Outputs
  SELECT
    subq_35.account_id__customer_id__customer_third_hop_id__txn_count AS customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count
    , third_hop_table_src_22000.customer_third_hop_id AS third_hop_count
  FROM ***************************.third_hop_table third_hop_table_src_22000
  LEFT OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['txn_count', 'account_id__customer_id__customer_third_hop_id']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['account_id__customer_id__customer_third_hop_id', 'account_id__customer_id__customer_third_hop_id__txn_count']
    SELECT
      subq_30.customer_id__customer_third_hop_id AS account_id__customer_id__customer_third_hop_id
      , SUM(account_month_txns_src_22000.txn_count) AS account_id__customer_id__customer_third_hop_id__txn_count
    FROM ***************************.account_month_txns account_month_txns_src_22000
    LEFT OUTER JOIN (
      -- Join Standard Outputs
      -- Pass Only Elements: ['ds_partitioned__day', 'account_id', 'customer_id__customer_third_hop_id']
      SELECT
        date_trunc('day', bridge_table_src_22000.ds_partitioned) AS ds_partitioned__day
        , bridge_table_src_22000.account_id AS account_id
        , customer_other_data_src_22000.customer_third_hop_id AS customer_id__customer_third_hop_id
      FROM ***************************.bridge_table bridge_table_src_22000
      LEFT OUTER JOIN
        ***************************.customer_other_data customer_other_data_src_22000
      ON
        bridge_table_src_22000.customer_id = customer_other_data_src_22000.customer_id
    ) subq_30
    ON
      (
        account_month_txns_src_22000.account_id = subq_30.account_id
      ) AND (
        date_trunc('day', account_month_txns_src_22000.ds_partitioned) = subq_30.ds_partitioned__day
      )
    GROUP BY
      account_id__customer_id__customer_third_hop_id
  ) subq_35
  ON
    third_hop_table_src_22000.customer_third_hop_id = subq_35.account_id__customer_id__customer_third_hop_id
) subq_36
WHERE customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count > 2
