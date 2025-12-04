test_name: test_inner_query_single_hop
test_filename: test_metric_filter_rendering.py
docstring:
  Tests rendering for a metric filter using a one-hop join in the inner query.
sql_engine: Databricks
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__third_hop_count']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  COUNT(DISTINCT third_hop_count) AS third_hop_count
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__third_hop_count', 'customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers']
  SELECT
    subq_30.customer_id__customer_third_hop_id__paraguayan_customers AS customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
    , third_hop_table_src_22000.customer_third_hop_id AS third_hop_count
  FROM ***************************.third_hop_table third_hop_table_src_22000
  LEFT OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__paraguayan_customers', 'customer_id__customer_third_hop_id']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['customer_id__customer_third_hop_id', 'customer_id__customer_third_hop_id__paraguayan_customers']
    SELECT
      customer_id__customer_third_hop_id
      , SUM(paraguayan_customers) AS customer_id__customer_third_hop_id__paraguayan_customers
    FROM (
      -- Read Elements From Semantic Model 'customer_other_data'
      -- Metric Time Dimension 'acquired_ds'
      -- Pass Only Elements: ['__paraguayan_customers', 'customer_id__country', 'customer_id__customer_third_hop_id']
      SELECT
        customer_third_hop_id AS customer_id__customer_third_hop_id
        , country AS customer_id__country
        , 1 AS paraguayan_customers
      FROM ***************************.customer_other_data customer_other_data_src_22000
    ) subq_25
    WHERE customer_id__country = 'paraguay'
    GROUP BY
      customer_id__customer_third_hop_id
  ) subq_30
  ON
    third_hop_table_src_22000.customer_third_hop_id = subq_30.customer_id__customer_third_hop_id
) subq_32
WHERE customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers > 0
