-- Join Standard Outputs
-- Pass Only Elements:
--   ['txn_count', 'account_id__customer_id__customer_name']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_18.customer_id__customer_name AS account_id__customer_id__customer_name
  , SUM(account_month_txns_src_10010.txn_count) AS txn_count
FROM ***************************.account_month_txns account_month_txns_src_10010
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['customer_id__customer_name', 'ds_partitioned__day', 'account_id']
  SELECT
    DATE_TRUNC(bridge_table_src_10011.ds_partitioned, day) AS ds_partitioned__day
    , bridge_table_src_10011.account_id AS account_id
    , customer_table_src_10013.customer_name AS customer_id__customer_name
  FROM ***************************.bridge_table bridge_table_src_10011
  LEFT OUTER JOIN
    ***************************.customer_table customer_table_src_10013
  ON
    (
      bridge_table_src_10011.customer_id = customer_table_src_10013.customer_id
    ) AND (
      DATE_TRUNC(bridge_table_src_10011.ds_partitioned, day) = DATE_TRUNC(customer_table_src_10013.ds_partitioned, day)
    )
) subq_18
ON
  (
    account_month_txns_src_10010.account_id = subq_18.account_id
  ) AND (
    DATE_TRUNC(account_month_txns_src_10010.ds_partitioned, day) = subq_18.ds_partitioned__day
  )
GROUP BY
  account_id__customer_id__customer_name
