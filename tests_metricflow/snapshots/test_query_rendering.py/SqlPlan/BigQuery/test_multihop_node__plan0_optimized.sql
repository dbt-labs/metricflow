test_name: test_multihop_node
test_filename: test_query_rendering.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a join between 1 measure and 2 dimensions.
sql_engine: BigQuery
---
-- Join Standard Outputs
-- Pass Only Elements: ['txn_count', 'account_id__customer_id__customer_name']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_24.customer_id__customer_name AS account_id__customer_id__customer_name
  , SUM(account_month_txns_src_22000.txn_count) AS txn_count
FROM ***************************.account_month_txns account_month_txns_src_22000
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements: ['customer_id__customer_name', 'ds_partitioned__day', 'account_id']
  SELECT
    TIMESTAMP_TRUNC(bridge_table_src_22000.ds_partitioned, day) AS ds_partitioned__day
    , bridge_table_src_22000.account_id AS account_id
    , customer_table_src_22000.customer_name AS customer_id__customer_name
  FROM ***************************.bridge_table bridge_table_src_22000
  LEFT OUTER JOIN
    ***************************.customer_table customer_table_src_22000
  ON
    (
      bridge_table_src_22000.customer_id = customer_table_src_22000.customer_id
    ) AND (
      TIMESTAMP_TRUNC(bridge_table_src_22000.ds_partitioned, day) = TIMESTAMP_TRUNC(customer_table_src_22000.ds_partitioned, day)
    )
) subq_24
ON
  (
    account_month_txns_src_22000.account_id = subq_24.account_id
  ) AND (
    TIMESTAMP_TRUNC(account_month_txns_src_22000.ds_partitioned, day) = subq_24.ds_partitioned__day
  )
GROUP BY
  account_id__customer_id__customer_name
