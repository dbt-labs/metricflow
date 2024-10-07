-- Constrain Output with WHERE
-- Pass Only Elements: ['third_hop_count',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  COUNT(DISTINCT third_hop_count) AS third_hop_count
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers']
  SELECT
    third_hop_table_src_22000.customer_third_hop_id AS third_hop_count
  FROM ***************************.third_hop_table third_hop_table_src_22000
  LEFT OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['customers_with_other_data', 'customer_id__customer_third_hop_id']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['customer_id__customer_third_hop_id', 'customer_id__customer_third_hop_id__paraguayan_customers']
    SELECT
      customer_id__customer_third_hop_id
    FROM (
      -- Read Elements From Semantic Model 'customer_other_data'
      -- Metric Time Dimension 'acquired_ds'
      -- Pass Only Elements: ['customers_with_other_data', 'customer_id__country', 'customer_id__customer_third_hop_id']
      SELECT
        customer_third_hop_id AS customer_id__customer_third_hop_id
        , country AS customer_id__country
      FROM ***************************.customer_other_data customer_other_data_src_22000
    ) subq_13
    WHERE customer_id__country = 'paraguay'
    GROUP BY
      customer_id__customer_third_hop_id
  ) subq_16
  ON
    third_hop_table_src_22000.customer_third_hop_id = subq_16.customer_id__customer_third_hop_id
) subq_17
WHERE customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers > 0
