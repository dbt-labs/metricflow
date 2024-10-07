-- Read Elements From Semantic Model 'third_hop_table'
-- Metric Time Dimension 'third_hop_ds'
-- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id']
-- Join Standard Outputs
-- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id__account_id__customer_id__customer_third_hop_id__txn_count']
-- Constrain Output with WHERE
-- Pass Only Elements: ['third_hop_count',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  COUNT(DISTINCT customer_third_hop_id) AS third_hop_count
FROM ***************************.third_hop_table third_hop_table_src_22000
