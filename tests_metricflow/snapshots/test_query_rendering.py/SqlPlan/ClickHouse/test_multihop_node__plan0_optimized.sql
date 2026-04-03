test_name: test_multihop_node
test_filename: test_query_rendering.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a join between 1 simple-metric input and 2 dimensions.
sql_engine: ClickHouse
---
SELECT
  subq_28.customer_id__customer_name AS account_id__customer_id__customer_name
  , SUM(account_month_txns_src_22000.txn_count) AS txn_count
FROM ***************************.account_month_txns account_month_txns_src_22000
LEFT OUTER JOIN (
  SELECT
    toStartOfDay(bridge_table_src_22000.ds_partitioned) AS ds_partitioned__day
    , bridge_table_src_22000.account_id AS account_id
    , customer_table_src_22000.customer_name AS customer_id__customer_name
  FROM ***************************.bridge_table bridge_table_src_22000
  LEFT OUTER JOIN
    ***************************.customer_table customer_table_src_22000
  ON
    (
      bridge_table_src_22000.customer_id = customer_table_src_22000.customer_id
    ) AND (
      toStartOfDay(bridge_table_src_22000.ds_partitioned) = toStartOfDay(customer_table_src_22000.ds_partitioned)
    )
) subq_28
ON
  (
    account_month_txns_src_22000.account_id = subq_28.account_id
  ) AND (
    toStartOfDay(account_month_txns_src_22000.ds_partitioned) = subq_28.ds_partitioned__day
  )
GROUP BY
  subq_28.customer_id__customer_name
