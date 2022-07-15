-- Join Standard Outputs
-- Pass Only Elements:
--   ['txn_count', 'account_id__customer_id__customer_name']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(account_month_txns_src_10009.txn_count) AS txn_count
  , subq_18.customer_id__customer_name AS account_id__customer_id__customer_name
FROM ***************************.account_month_txns account_month_txns_src_10009
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['account_id', 'ds_partitioned', 'customer_id__customer_name']
  SELECT
    customer_table_src_10012.customer_name AS customer_id__customer_name
    , bridge_table_src_10010.ds_partitioned AS ds_partitioned
    , bridge_table_src_10010.account_id AS account_id
  FROM ***************************.bridge_table bridge_table_src_10010
  LEFT OUTER JOIN
    ***************************.customer_table customer_table_src_10012
  ON
    (
      bridge_table_src_10010.customer_id = customer_table_src_10012.customer_id
    ) AND (
      bridge_table_src_10010.ds_partitioned = customer_table_src_10012.ds_partitioned
    )
) subq_18
ON
  (
    account_month_txns_src_10009.account_id = subq_18.account_id
  ) AND (
    account_month_txns_src_10009.ds_partitioned = subq_18.ds_partitioned
  )
GROUP BY
  subq_18.customer_id__customer_name
