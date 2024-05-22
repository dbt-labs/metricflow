-- Compute Metrics via Expressions
SELECT
  subq_27.third_hop_count
FROM (
  -- Aggregate Measures
  SELECT
    COUNT(DISTINCT subq_26.third_hop_count) AS third_hop_count
  FROM (
    -- Pass Only Elements: ['third_hop_count',]
    SELECT
      subq_25.third_hop_count
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_24.customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
        , subq_24.third_hop_count
      FROM (
        -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers']
        SELECT
          subq_23.customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
          , subq_23.third_hop_count
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_13.customer_third_hop_id AS customer_third_hop_id
            , subq_22.customer_id__customer_third_hop_id AS customer_third_hop_id__customer_id__customer_third_hop_id
            , subq_22.customer_id__customer_third_hop_id__paraguayan_customers AS customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
            , subq_13.third_hop_count AS third_hop_count
          FROM (
            -- Pass Only Elements: ['third_hop_count', 'customer_third_hop_id']
            SELECT
              subq_12.customer_third_hop_id
              , subq_12.third_hop_count
            FROM (
              -- Metric Time Dimension 'third_hop_ds'
              SELECT
                subq_11.third_hop_ds__day
                , subq_11.third_hop_ds__week
                , subq_11.third_hop_ds__month
                , subq_11.third_hop_ds__quarter
                , subq_11.third_hop_ds__year
                , subq_11.third_hop_ds__extract_year
                , subq_11.third_hop_ds__extract_quarter
                , subq_11.third_hop_ds__extract_month
                , subq_11.third_hop_ds__extract_day
                , subq_11.third_hop_ds__extract_dow
                , subq_11.third_hop_ds__extract_doy
                , subq_11.customer_third_hop_id__third_hop_ds__day
                , subq_11.customer_third_hop_id__third_hop_ds__week
                , subq_11.customer_third_hop_id__third_hop_ds__month
                , subq_11.customer_third_hop_id__third_hop_ds__quarter
                , subq_11.customer_third_hop_id__third_hop_ds__year
                , subq_11.customer_third_hop_id__third_hop_ds__extract_year
                , subq_11.customer_third_hop_id__third_hop_ds__extract_quarter
                , subq_11.customer_third_hop_id__third_hop_ds__extract_month
                , subq_11.customer_third_hop_id__third_hop_ds__extract_day
                , subq_11.customer_third_hop_id__third_hop_ds__extract_dow
                , subq_11.customer_third_hop_id__third_hop_ds__extract_doy
                , subq_11.third_hop_ds__day AS metric_time__day
                , subq_11.third_hop_ds__week AS metric_time__week
                , subq_11.third_hop_ds__month AS metric_time__month
                , subq_11.third_hop_ds__quarter AS metric_time__quarter
                , subq_11.third_hop_ds__year AS metric_time__year
                , subq_11.third_hop_ds__extract_year AS metric_time__extract_year
                , subq_11.third_hop_ds__extract_quarter AS metric_time__extract_quarter
                , subq_11.third_hop_ds__extract_month AS metric_time__extract_month
                , subq_11.third_hop_ds__extract_day AS metric_time__extract_day
                , subq_11.third_hop_ds__extract_dow AS metric_time__extract_dow
                , subq_11.third_hop_ds__extract_doy AS metric_time__extract_doy
                , subq_11.customer_third_hop_id
                , subq_11.value
                , subq_11.customer_third_hop_id__value
                , subq_11.third_hop_count
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
                  , CASE WHEN EXTRACT(dow FROM third_hop_table_src_22000.third_hop_ds) = 0 THEN EXTRACT(dow FROM third_hop_table_src_22000.third_hop_ds) + 7 ELSE EXTRACT(dow FROM third_hop_table_src_22000.third_hop_ds) END AS third_hop_ds__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM third_hop_table_src_22000.third_hop_ds) = 0 THEN EXTRACT(dow FROM third_hop_table_src_22000.third_hop_ds) + 7 ELSE EXTRACT(dow FROM third_hop_table_src_22000.third_hop_ds) END AS customer_third_hop_id__third_hop_ds__extract_dow
                  , EXTRACT(doy FROM third_hop_table_src_22000.third_hop_ds) AS customer_third_hop_id__third_hop_ds__extract_doy
                  , third_hop_table_src_22000.customer_third_hop_id
                FROM ***************************.third_hop_table third_hop_table_src_22000
              ) subq_11
            ) subq_12
          ) subq_13
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['customer_id__customer_third_hop_id', 'customer_id__customer_third_hop_id__paraguayan_customers']
            SELECT
              subq_21.customer_id__customer_third_hop_id
              , subq_21.customer_id__customer_third_hop_id__paraguayan_customers
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_20.customer_id__customer_third_hop_id
                , subq_20.customers_with_other_data AS customer_id__customer_third_hop_id__paraguayan_customers
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_19.customer_id__customer_third_hop_id
                  , SUM(subq_19.customers_with_other_data) AS customers_with_other_data
                FROM (
                  -- Pass Only Elements: ['customers_with_other_data', 'customer_id__customer_third_hop_id']
                  SELECT
                    subq_18.customer_id__customer_third_hop_id
                    , subq_18.customers_with_other_data
                  FROM (
                    -- Constrain Output with WHERE
                    SELECT
                      subq_17.customer_id__customer_third_hop_id
                      , subq_17.customer_id__country
                      , subq_17.customers_with_other_data
                    FROM (
                      -- Pass Only Elements: ['customers_with_other_data', 'customer_id__country', 'customer_id__customer_third_hop_id']
                      SELECT
                        subq_16.customer_id__customer_third_hop_id
                        , subq_16.customer_id__country
                        , subq_16.customers_with_other_data
                      FROM (
                        -- Constrain Output with WHERE
                        SELECT
                          subq_15.acquired_ds__day
                          , subq_15.acquired_ds__week
                          , subq_15.acquired_ds__month
                          , subq_15.acquired_ds__quarter
                          , subq_15.acquired_ds__year
                          , subq_15.acquired_ds__extract_year
                          , subq_15.acquired_ds__extract_quarter
                          , subq_15.acquired_ds__extract_month
                          , subq_15.acquired_ds__extract_day
                          , subq_15.acquired_ds__extract_dow
                          , subq_15.acquired_ds__extract_doy
                          , subq_15.customer_id__acquired_ds__day
                          , subq_15.customer_id__acquired_ds__week
                          , subq_15.customer_id__acquired_ds__month
                          , subq_15.customer_id__acquired_ds__quarter
                          , subq_15.customer_id__acquired_ds__year
                          , subq_15.customer_id__acquired_ds__extract_year
                          , subq_15.customer_id__acquired_ds__extract_quarter
                          , subq_15.customer_id__acquired_ds__extract_month
                          , subq_15.customer_id__acquired_ds__extract_day
                          , subq_15.customer_id__acquired_ds__extract_dow
                          , subq_15.customer_id__acquired_ds__extract_doy
                          , subq_15.customer_third_hop_id__acquired_ds__day
                          , subq_15.customer_third_hop_id__acquired_ds__week
                          , subq_15.customer_third_hop_id__acquired_ds__month
                          , subq_15.customer_third_hop_id__acquired_ds__quarter
                          , subq_15.customer_third_hop_id__acquired_ds__year
                          , subq_15.customer_third_hop_id__acquired_ds__extract_year
                          , subq_15.customer_third_hop_id__acquired_ds__extract_quarter
                          , subq_15.customer_third_hop_id__acquired_ds__extract_month
                          , subq_15.customer_third_hop_id__acquired_ds__extract_day
                          , subq_15.customer_third_hop_id__acquired_ds__extract_dow
                          , subq_15.customer_third_hop_id__acquired_ds__extract_doy
                          , subq_15.metric_time__day
                          , subq_15.metric_time__week
                          , subq_15.metric_time__month
                          , subq_15.metric_time__quarter
                          , subq_15.metric_time__year
                          , subq_15.metric_time__extract_year
                          , subq_15.metric_time__extract_quarter
                          , subq_15.metric_time__extract_month
                          , subq_15.metric_time__extract_day
                          , subq_15.metric_time__extract_dow
                          , subq_15.metric_time__extract_doy
                          , subq_15.customer_id
                          , subq_15.customer_third_hop_id
                          , subq_15.customer_id__customer_third_hop_id
                          , subq_15.customer_third_hop_id__customer_id
                          , subq_15.country
                          , subq_15.customer_id__country
                          , subq_15.customer_third_hop_id__country
                          , subq_15.customers_with_other_data
                        FROM (
                          -- Metric Time Dimension 'acquired_ds'
                          SELECT
                            subq_14.acquired_ds__day
                            , subq_14.acquired_ds__week
                            , subq_14.acquired_ds__month
                            , subq_14.acquired_ds__quarter
                            , subq_14.acquired_ds__year
                            , subq_14.acquired_ds__extract_year
                            , subq_14.acquired_ds__extract_quarter
                            , subq_14.acquired_ds__extract_month
                            , subq_14.acquired_ds__extract_day
                            , subq_14.acquired_ds__extract_dow
                            , subq_14.acquired_ds__extract_doy
                            , subq_14.customer_id__acquired_ds__day
                            , subq_14.customer_id__acquired_ds__week
                            , subq_14.customer_id__acquired_ds__month
                            , subq_14.customer_id__acquired_ds__quarter
                            , subq_14.customer_id__acquired_ds__year
                            , subq_14.customer_id__acquired_ds__extract_year
                            , subq_14.customer_id__acquired_ds__extract_quarter
                            , subq_14.customer_id__acquired_ds__extract_month
                            , subq_14.customer_id__acquired_ds__extract_day
                            , subq_14.customer_id__acquired_ds__extract_dow
                            , subq_14.customer_id__acquired_ds__extract_doy
                            , subq_14.customer_third_hop_id__acquired_ds__day
                            , subq_14.customer_third_hop_id__acquired_ds__week
                            , subq_14.customer_third_hop_id__acquired_ds__month
                            , subq_14.customer_third_hop_id__acquired_ds__quarter
                            , subq_14.customer_third_hop_id__acquired_ds__year
                            , subq_14.customer_third_hop_id__acquired_ds__extract_year
                            , subq_14.customer_third_hop_id__acquired_ds__extract_quarter
                            , subq_14.customer_third_hop_id__acquired_ds__extract_month
                            , subq_14.customer_third_hop_id__acquired_ds__extract_day
                            , subq_14.customer_third_hop_id__acquired_ds__extract_dow
                            , subq_14.customer_third_hop_id__acquired_ds__extract_doy
                            , subq_14.acquired_ds__day AS metric_time__day
                            , subq_14.acquired_ds__week AS metric_time__week
                            , subq_14.acquired_ds__month AS metric_time__month
                            , subq_14.acquired_ds__quarter AS metric_time__quarter
                            , subq_14.acquired_ds__year AS metric_time__year
                            , subq_14.acquired_ds__extract_year AS metric_time__extract_year
                            , subq_14.acquired_ds__extract_quarter AS metric_time__extract_quarter
                            , subq_14.acquired_ds__extract_month AS metric_time__extract_month
                            , subq_14.acquired_ds__extract_day AS metric_time__extract_day
                            , subq_14.acquired_ds__extract_dow AS metric_time__extract_dow
                            , subq_14.acquired_ds__extract_doy AS metric_time__extract_doy
                            , subq_14.customer_id
                            , subq_14.customer_third_hop_id
                            , subq_14.customer_id__customer_third_hop_id
                            , subq_14.customer_third_hop_id__customer_id
                            , subq_14.country
                            , subq_14.customer_id__country
                            , subq_14.customer_third_hop_id__country
                            , subq_14.customers_with_other_data
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
                              , CASE WHEN EXTRACT(dow FROM customer_other_data_src_22000.acquired_ds) = 0 THEN EXTRACT(dow FROM customer_other_data_src_22000.acquired_ds) + 7 ELSE EXTRACT(dow FROM customer_other_data_src_22000.acquired_ds) END AS acquired_ds__extract_dow
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
                              , CASE WHEN EXTRACT(dow FROM customer_other_data_src_22000.acquired_ds) = 0 THEN EXTRACT(dow FROM customer_other_data_src_22000.acquired_ds) + 7 ELSE EXTRACT(dow FROM customer_other_data_src_22000.acquired_ds) END AS customer_id__acquired_ds__extract_dow
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
                              , CASE WHEN EXTRACT(dow FROM customer_other_data_src_22000.acquired_ds) = 0 THEN EXTRACT(dow FROM customer_other_data_src_22000.acquired_ds) + 7 ELSE EXTRACT(dow FROM customer_other_data_src_22000.acquired_ds) END AS customer_third_hop_id__acquired_ds__extract_dow
                              , EXTRACT(doy FROM customer_other_data_src_22000.acquired_ds) AS customer_third_hop_id__acquired_ds__extract_doy
                              , customer_other_data_src_22000.customer_id
                              , customer_other_data_src_22000.customer_third_hop_id
                              , customer_other_data_src_22000.customer_third_hop_id AS customer_id__customer_third_hop_id
                              , customer_other_data_src_22000.customer_id AS customer_third_hop_id__customer_id
                            FROM ***************************.customer_other_data customer_other_data_src_22000
                          ) subq_14
                        ) subq_15
                        WHERE customer_id__country = 'paraguay'
                      ) subq_16
                    ) subq_17
                    WHERE customer_id__country = 'paraguay'
                  ) subq_18
                ) subq_19
                GROUP BY
                  subq_19.customer_id__customer_third_hop_id
              ) subq_20
            ) subq_21
          ) subq_22
          ON
            subq_13.customer_third_hop_id = subq_22.customer_id__customer_third_hop_id
        ) subq_23
      ) subq_24
      WHERE customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers > 0
    ) subq_25
  ) subq_26
) subq_27
