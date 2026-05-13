test_name: test_inner_query_single_hop
test_filename: test_metric_filter_rendering.py
docstring:
  Tests rendering for a metric filter using a one-hop join in the inner query.
sql_engine: ClickHouse
---
SELECT
  COUNT(DISTINCT __third_hop_count) AS third_hop_count
FROM (
  SELECT
    third_hop_count AS __third_hop_count
  FROM (
    SELECT
      subq_35.customer_id__customer_third_hop_id__paraguayan_customers AS customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
      , third_hop_table_src_22000.customer_third_hop_id AS third_hop_count
    FROM ***************************.third_hop_table third_hop_table_src_22000
    LEFT OUTER JOIN (
      SELECT
        customer_id__customer_third_hop_id
        , SUM(__paraguayan_customers) AS customer_id__customer_third_hop_id__paraguayan_customers
      FROM (
        SELECT
          customer_id__customer_third_hop_id
          , paraguayan_customers AS __paraguayan_customers
        FROM (
          SELECT
            customer_third_hop_id AS customer_id__customer_third_hop_id
            , country AS customer_id__country
            , 1 AS paraguayan_customers
          FROM ***************************.customer_other_data customer_other_data_src_22000
        ) subq_30
        WHERE customer_id__country = 'paraguay'
      ) subq_32
      GROUP BY
        customer_id__customer_third_hop_id
    ) subq_35
    ON
      third_hop_table_src_22000.customer_third_hop_id = subq_35.customer_id__customer_third_hop_id
  ) subq_37
  WHERE customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers > 0
) subq_39
