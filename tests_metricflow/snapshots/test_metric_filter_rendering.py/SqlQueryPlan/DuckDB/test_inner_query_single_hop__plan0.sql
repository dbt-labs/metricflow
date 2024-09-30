-- Compute Metrics via Expressions
SELECT
  subq_9.third_hop_count
FROM (
  -- Aggregate Measures
  SELECT
    COUNT(DISTINCT subq_8.third_hop_count) AS third_hop_count
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['third_hop_count',]
    SELECT
      subq_7.third_hop_count
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers']
      SELECT
        subq_1.third_hop_count AS third_hop_count
      FROM (
        -- Metric Time Dimension 'third_hop_ds'
        -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id']
        SELECT
          subq_0.customer_third_hop_id
          , subq_0.third_hop_count
        FROM (
          -- Read Elements From Semantic Model 'third_hop_table'
          SELECT
            third_hop_table_src_22000.customer_third_hop_id AS third_hop_count
            , third_hop_table_src_22000.value
            , DATE_TRUNC('day', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__day
            , DATE_TRUNC('week', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__week
            , DATE_TRUNC('month', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__month
            , DATE_TRUNC('quarter', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__quarter
            , DATE_TRUNC('year', third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__year
            , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_year
            , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_quarter
            , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_month
            , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_day
            , EXTRACT(isodow FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_dow
            , EXTRACT(doy FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_doy
            , third_hop_table_src_22000.value AS customer_third_hop_id__value
            , DATE_TRUNC('day', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__day
            , DATE_TRUNC('week', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__week
            , DATE_TRUNC('month', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__month
            , DATE_TRUNC('quarter', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__quarter
            , DATE_TRUNC('year', third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__year
            , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_year
            , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_quarter
            , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_month
            , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_day
            , EXTRACT(isodow FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_dow
            , EXTRACT(doy FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_doy
            , third_hop_table_src_22000.customer_third_hop_id
          FROM ***************************.third_hop_table third_hop_table_src_22000
        ) subq_0
      ) subq_1
      LEFT OUTER JOIN (
        -- Compute Metrics via Expressions
        -- Pass Only Elements: ['customer_id__customer_third_hop_id', 'customer_id__customer_third_hop_id__paraguayan_customers']
        SELECT
          subq_5.customer_id__customer_third_hop_id
          , subq_5.customers_with_other_data AS customer_id__customer_third_hop_id__paraguayan_customers
        FROM (
          -- Aggregate Measures
          SELECT
            subq_4.customer_id__customer_third_hop_id
            , SUM(subq_4.customers_with_other_data) AS customers_with_other_data
          FROM (
            -- Constrain Output with WHERE
            -- Pass Only Elements: ['customers_with_other_data', 'customer_id__customer_third_hop_id']
            SELECT
              subq_3.customer_id__customer_third_hop_id
              , subq_3.customers_with_other_data
            FROM (
              -- Metric Time Dimension 'acquired_ds'
              -- Pass Only Elements: ['customers_with_other_data', 'customer_id__country', 'customer_id__customer_third_hop_id']
              SELECT
                subq_2.customer_id__customer_third_hop_id
                , subq_2.customer_id__country
                , subq_2.customers_with_other_data
              FROM (
                -- Read Elements From Semantic Model 'customer_other_data'
                SELECT
                  1 AS customers_with_other_data
                  , customer_other_data_src_22000.country
                  , DATE_TRUNC('day', customer_other_data_src_22000.acquired_ds) AS acquired_ds__day
                  , DATE_TRUNC('week', customer_other_data_src_22000.acquired_ds) AS acquired_ds__week
                  , DATE_TRUNC('month', customer_other_data_src_22000.acquired_ds) AS acquired_ds__month
                  , DATE_TRUNC('quarter', customer_other_data_src_22000.acquired_ds) AS acquired_ds__quarter
                  , DATE_TRUNC('year', customer_other_data_src_22000.acquired_ds) AS acquired_ds__year
                  , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_year
                  , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_quarter
                  , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_month
                  , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_day
                  , EXTRACT(isodow FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_dow
                  , EXTRACT(doy FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_doy
                  , customer_other_data_src_22000.country AS customer_id__country
                  , DATE_TRUNC('day', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__day
                  , DATE_TRUNC('week', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__week
                  , DATE_TRUNC('month', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__month
                  , DATE_TRUNC('quarter', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__quarter
                  , DATE_TRUNC('year', customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__year
                  , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_year
                  , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_quarter
                  , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_month
                  , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_day
                  , EXTRACT(isodow FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_dow
                  , EXTRACT(doy FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_doy
                  , customer_other_data_src_22000.country AS customer_third_hop_id__country
                  , DATE_TRUNC('day', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__day
                  , DATE_TRUNC('week', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__week
                  , DATE_TRUNC('month', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__month
                  , DATE_TRUNC('quarter', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__quarter
                  , DATE_TRUNC('year', customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__year
                  , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_year
                  , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_quarter
                  , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_month
                  , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_day
                  , EXTRACT(isodow FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_dow
                  , EXTRACT(doy FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_doy
                  , customer_other_data_src_22000.customer_id
                  , customer_other_data_src_22000.customer_third_hop_id
                  , customer_other_data_src_22000.customer_third_hop_id AS customer_id__customer_third_hop_id
                  , customer_other_data_src_22000.customer_id AS customer_third_hop_id__customer_id
                FROM ***************************.customer_other_data customer_other_data_src_22000
              ) subq_2
            ) subq_3
            WHERE customer_id__country = 'paraguay'
          ) subq_4
          GROUP BY
            subq_4.customer_id__customer_third_hop_id
        ) subq_5
      ) subq_6
      ON
        subq_1.customer_third_hop_id = subq_6.customer_id__customer_third_hop_id
    ) subq_7
    WHERE customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers > 0
  ) subq_8
) subq_9
