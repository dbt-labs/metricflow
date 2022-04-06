-- Join Standard Outputs
-- Pass Only Elements:
--   ['txn_count', 'account_id__customer_id__customer_name']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(account_month_txns_src_0.txn_count) AS txn_count
  , subq_16.customer_id__customer_name AS account_id__customer_id__customer_name
FROM ***************************.account_month_txns account_month_txns_src_0
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['account_id', 'ds_partitioned', 'customer_id__customer_name']
  SELECT
    customer_table_src_3.customer_name AS customer_id__customer_name
    , bridge_table_src_1.ds_partitioned AS ds_partitioned
    , bridge_table_src_1.account_id AS account_id
  FROM ***************************.bridge_table bridge_table_src_1
  LEFT OUTER JOIN
    ***************************.customer_table customer_table_src_3
  ON
    (
      bridge_table_src_1.customer_id = customer_table_src_3.customer_id
    ) AND (
      bridge_table_src_1.ds_partitioned = customer_table_src_3.ds_partitioned
    )
) subq_16
ON
  (
    account_month_txns_src_0.account_id = subq_16.account_id
  ) AND (
    account_month_txns_src_0.ds_partitioned = subq_16.ds_partitioned
  )
GROUP BY
  account_id__customer_id__customer_name
