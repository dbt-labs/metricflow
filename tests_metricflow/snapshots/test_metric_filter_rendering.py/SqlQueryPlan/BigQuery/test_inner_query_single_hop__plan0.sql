-- Compute Metrics via Expressions
SELECT
  subq_15.third_hop_count
FROM (
  -- Aggregate Measures
  SELECT
    COUNT(DISTINCT subq_14.third_hop_count) AS third_hop_count
  FROM (
    -- Pass Only Elements: ['third_hop_count',]
    SELECT
      subq_13.third_hop_count
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_12.customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
        , subq_12.third_hop_count
      FROM (
        -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers']
        SELECT
          subq_11.customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
          , subq_11.third_hop_count
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_2.customer_third_hop_id AS customer_third_hop_id
            , subq_10.customer_id__customer_third_hop_id AS customer_third_hop_id__customer_id__customer_third_hop_id
            , subq_10.customer_id__customer_third_hop_id__paraguayan_customers AS customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
            , subq_2.third_hop_count AS third_hop_count
          FROM (
            -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id']
            SELECT
              subq_1.customer_third_hop_id
              , subq_1.third_hop_count
            FROM (
              -- Metric Time Dimension 'third_hop_ds'
              SELECT
                subq_0.third_hop_ds__day
                , subq_0.third_hop_ds__week
                , subq_0.third_hop_ds__month
                , subq_0.third_hop_ds__quarter
                , subq_0.third_hop_ds__year
                , subq_0.third_hop_ds__extract_year
                , subq_0.third_hop_ds__extract_quarter
                , subq_0.third_hop_ds__extract_month
                , subq_0.third_hop_ds__extract_day
                , subq_0.third_hop_ds__extract_dow
                , subq_0.third_hop_ds__extract_doy
                , subq_0.customer_third_hop_id__third_hop_ds__day
                , subq_0.customer_third_hop_id__third_hop_ds__week
                , subq_0.customer_third_hop_id__third_hop_ds__month
                , subq_0.customer_third_hop_id__third_hop_ds__quarter
                , subq_0.customer_third_hop_id__third_hop_ds__year
                , subq_0.customer_third_hop_id__third_hop_ds__extract_year
                , subq_0.customer_third_hop_id__third_hop_ds__extract_quarter
                , subq_0.customer_third_hop_id__third_hop_ds__extract_month
                , subq_0.customer_third_hop_id__third_hop_ds__extract_day
                , subq_0.customer_third_hop_id__third_hop_ds__extract_dow
                , subq_0.customer_third_hop_id__third_hop_ds__extract_doy
                , subq_0.third_hop_ds__day AS metric_time__day
                , subq_0.third_hop_ds__week AS metric_time__week
                , subq_0.third_hop_ds__month AS metric_time__month
                , subq_0.third_hop_ds__quarter AS metric_time__quarter
                , subq_0.third_hop_ds__year AS metric_time__year
                , subq_0.third_hop_ds__extract_year AS metric_time__extract_year
                , subq_0.third_hop_ds__extract_quarter AS metric_time__extract_quarter
                , subq_0.third_hop_ds__extract_month AS metric_time__extract_month
                , subq_0.third_hop_ds__extract_day AS metric_time__extract_day
                , subq_0.third_hop_ds__extract_dow AS metric_time__extract_dow
                , subq_0.third_hop_ds__extract_doy AS metric_time__extract_doy
                , subq_0.customer_third_hop_id
                , subq_0.value
                , subq_0.customer_third_hop_id__value
                , subq_0.third_hop_count
              FROM (
                -- Read Elements From Semantic Model 'third_hop_table'
                SELECT
                  third_hop_table_src_22000.customer_third_hop_id AS third_hop_count
                  , third_hop_table_src_22000.value
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, day) AS third_hop_ds__day
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, isoweek) AS third_hop_ds__week
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, month) AS third_hop_ds__month
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, quarter) AS third_hop_ds__quarter
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, year) AS third_hop_ds__year
                  , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_year
                  , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_quarter
                  , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_month
                  , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_day
                  , IF(EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) = 1, 7, EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) - 1) AS third_hop_ds__extract_dow
                  , EXTRACT(dayofyear FROM third_hop_table_src_22000.third_hop_ds) AS third_hop_ds__extract_doy
                  , third_hop_table_src_22000.value AS customer_third_hop_id__value
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, day) AS customer_third_hop_id__third_hop_ds__day
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, isoweek) AS customer_third_hop_id__third_hop_ds__week
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, month) AS customer_third_hop_id__third_hop_ds__month
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, quarter) AS customer_third_hop_id__third_hop_ds__quarter
                  , DATETIME_TRUNC(third_hop_table_src_22000.third_hop_ds, year) AS customer_third_hop_id__third_hop_ds__year
                  , EXTRACT(year FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_year
                  , EXTRACT(quarter FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_quarter
                  , EXTRACT(month FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_month
                  , EXTRACT(day FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_day
                  , IF(EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) = 1, 7, EXTRACT(dayofweek FROM third_hop_table_src_22000.third_hop_ds) - 1) AS customer_third_hop_id__third_hop_ds__extract_dow
                  , EXTRACT(dayofyear FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_doy
                  , third_hop_table_src_22000.customer_third_hop_id
                FROM ***************************.third_hop_table third_hop_table_src_22000
              ) subq_0
            ) subq_1
          ) subq_2
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['customer_id__customer_third_hop_id', 'customer_id__customer_third_hop_id__paraguayan_customers']
            SELECT
              subq_9.customer_id__customer_third_hop_id
              , subq_9.customer_id__customer_third_hop_id__paraguayan_customers
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_8.customer_id__customer_third_hop_id
                , subq_8.customers_with_other_data AS customer_id__customer_third_hop_id__paraguayan_customers
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_7.customer_id__customer_third_hop_id
                  , SUM(subq_7.customers_with_other_data) AS customers_with_other_data
                FROM (
                  -- Pass Only Elements: ['customers_with_other_data', 'customer_id__customer_third_hop_id']
                  SELECT
                    subq_6.customer_id__customer_third_hop_id
                    , subq_6.customers_with_other_data
                  FROM (
                    -- Constrain Output with WHERE
                    SELECT
                      subq_5.customer_id__customer_third_hop_id
                      , subq_5.customer_id__country
                      , subq_5.customers_with_other_data
                    FROM (
                      -- Pass Only Elements: ['customers_with_other_data', 'customer_id__country', 'customer_id__customer_third_hop_id']
                      SELECT
                        subq_4.customer_id__customer_third_hop_id
                        , subq_4.customer_id__country
                        , subq_4.customers_with_other_data
                      FROM (
                        -- Metric Time Dimension 'acquired_ds'
                        SELECT
                          subq_3.acquired_ds__day
                          , subq_3.acquired_ds__week
                          , subq_3.acquired_ds__month
                          , subq_3.acquired_ds__quarter
                          , subq_3.acquired_ds__year
                          , subq_3.acquired_ds__extract_year
                          , subq_3.acquired_ds__extract_quarter
                          , subq_3.acquired_ds__extract_month
                          , subq_3.acquired_ds__extract_day
                          , subq_3.acquired_ds__extract_dow
                          , subq_3.acquired_ds__extract_doy
                          , subq_3.customer_id__acquired_ds__day
                          , subq_3.customer_id__acquired_ds__week
                          , subq_3.customer_id__acquired_ds__month
                          , subq_3.customer_id__acquired_ds__quarter
                          , subq_3.customer_id__acquired_ds__year
                          , subq_3.customer_id__acquired_ds__extract_year
                          , subq_3.customer_id__acquired_ds__extract_quarter
                          , subq_3.customer_id__acquired_ds__extract_month
                          , subq_3.customer_id__acquired_ds__extract_day
                          , subq_3.customer_id__acquired_ds__extract_dow
                          , subq_3.customer_id__acquired_ds__extract_doy
                          , subq_3.customer_third_hop_id__acquired_ds__day
                          , subq_3.customer_third_hop_id__acquired_ds__week
                          , subq_3.customer_third_hop_id__acquired_ds__month
                          , subq_3.customer_third_hop_id__acquired_ds__quarter
                          , subq_3.customer_third_hop_id__acquired_ds__year
                          , subq_3.customer_third_hop_id__acquired_ds__extract_year
                          , subq_3.customer_third_hop_id__acquired_ds__extract_quarter
                          , subq_3.customer_third_hop_id__acquired_ds__extract_month
                          , subq_3.customer_third_hop_id__acquired_ds__extract_day
                          , subq_3.customer_third_hop_id__acquired_ds__extract_dow
                          , subq_3.customer_third_hop_id__acquired_ds__extract_doy
                          , subq_3.acquired_ds__day AS metric_time__day
                          , subq_3.acquired_ds__week AS metric_time__week
                          , subq_3.acquired_ds__month AS metric_time__month
                          , subq_3.acquired_ds__quarter AS metric_time__quarter
                          , subq_3.acquired_ds__year AS metric_time__year
                          , subq_3.acquired_ds__extract_year AS metric_time__extract_year
                          , subq_3.acquired_ds__extract_quarter AS metric_time__extract_quarter
                          , subq_3.acquired_ds__extract_month AS metric_time__extract_month
                          , subq_3.acquired_ds__extract_day AS metric_time__extract_day
                          , subq_3.acquired_ds__extract_dow AS metric_time__extract_dow
                          , subq_3.acquired_ds__extract_doy AS metric_time__extract_doy
                          , subq_3.customer_id
                          , subq_3.customer_third_hop_id
                          , subq_3.customer_id__customer_third_hop_id
                          , subq_3.customer_third_hop_id__customer_id
                          , subq_3.country
                          , subq_3.customer_id__country
                          , subq_3.customer_third_hop_id__country
                          , subq_3.customers_with_other_data
                        FROM (
                          -- Read Elements From Semantic Model 'customer_other_data'
                          SELECT
                            1 AS customers_with_other_data
                            , customer_other_data_src_22000.country
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, day) AS acquired_ds__day
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, isoweek) AS acquired_ds__week
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, month) AS acquired_ds__month
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, quarter) AS acquired_ds__quarter
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, year) AS acquired_ds__year
                            , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_year
                            , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_quarter
                            , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_month
                            , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_day
                            , IF(EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) = 1, 7, EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) - 1) AS acquired_ds__extract_dow
                            , EXTRACT(dayofyear FROM customer_other_data_src_22000.acquired_ds) AS acquired_ds__extract_doy
                            , customer_other_data_src_22000.country AS customer_id__country
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, day) AS customer_id__acquired_ds__day
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, isoweek) AS customer_id__acquired_ds__week
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, month) AS customer_id__acquired_ds__month
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, quarter) AS customer_id__acquired_ds__quarter
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, year) AS customer_id__acquired_ds__year
                            , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_year
                            , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_quarter
                            , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_month
                            , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_day
                            , IF(EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) = 1, 7, EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) - 1) AS customer_id__acquired_ds__extract_dow
                            , EXTRACT(dayofyear FROM customer_other_data_src_22000.acquired_ds) AS customer_id__acquired_ds__extract_doy
                            , customer_other_data_src_22000.country AS customer_third_hop_id__country
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, day) AS customer_third_hop_id__acquired_ds__day
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, isoweek) AS customer_third_hop_id__acquired_ds__week
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, month) AS customer_third_hop_id__acquired_ds__month
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, quarter) AS customer_third_hop_id__acquired_ds__quarter
                            , DATETIME_TRUNC(customer_other_data_src_22000.acquired_ds, year) AS customer_third_hop_id__acquired_ds__year
                            , EXTRACT(year FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_year
                            , EXTRACT(quarter FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_quarter
                            , EXTRACT(month FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_month
                            , EXTRACT(day FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_day
                            , IF(EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) = 1, 7, EXTRACT(dayofweek FROM customer_other_data_src_22000.acquired_ds) - 1) AS customer_third_hop_id__acquired_ds__extract_dow
                            , EXTRACT(dayofyear FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_doy
                            , customer_other_data_src_22000.customer_id
                            , customer_other_data_src_22000.customer_third_hop_id
                            , customer_other_data_src_22000.customer_third_hop_id AS customer_id__customer_third_hop_id
                            , customer_other_data_src_22000.customer_id AS customer_third_hop_id__customer_id
                          FROM ***************************.customer_other_data customer_other_data_src_22000
                        ) subq_3
                      ) subq_4
                    ) subq_5
                    WHERE customer_id__country = 'paraguay'
                  ) subq_6
                ) subq_7
                GROUP BY
                  customer_id__customer_third_hop_id
              ) subq_8
            ) subq_9
          ) subq_10
          ON
            subq_2.customer_third_hop_id = subq_10.customer_id__customer_third_hop_id
        ) subq_11
      ) subq_12
      WHERE customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers > 0
    ) subq_13
  ) subq_14
) subq_15
