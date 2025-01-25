test_name: test_inner_query_single_hop
test_filename: test_metric_filter_rendering.py
docstring:
  Tests rendering for a metric filter using a one-hop join in the inner query.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_14.third_hop_count
FROM (
  -- Aggregate Measures
  SELECT
    COUNT(DISTINCT nr_subq_13.third_hop_count) AS third_hop_count
  FROM (
    -- Pass Only Elements: ['third_hop_count',]
    SELECT
      nr_subq_12.third_hop_count
    FROM (
      -- Constrain Output with WHERE
      SELECT
        nr_subq_11.third_hop_ds__day
        , nr_subq_11.third_hop_ds__week
        , nr_subq_11.third_hop_ds__month
        , nr_subq_11.third_hop_ds__quarter
        , nr_subq_11.third_hop_ds__year
        , nr_subq_11.third_hop_ds__extract_year
        , nr_subq_11.third_hop_ds__extract_quarter
        , nr_subq_11.third_hop_ds__extract_month
        , nr_subq_11.third_hop_ds__extract_day
        , nr_subq_11.third_hop_ds__extract_dow
        , nr_subq_11.third_hop_ds__extract_doy
        , nr_subq_11.customer_third_hop_id__third_hop_ds__day
        , nr_subq_11.customer_third_hop_id__third_hop_ds__week
        , nr_subq_11.customer_third_hop_id__third_hop_ds__month
        , nr_subq_11.customer_third_hop_id__third_hop_ds__quarter
        , nr_subq_11.customer_third_hop_id__third_hop_ds__year
        , nr_subq_11.customer_third_hop_id__third_hop_ds__extract_year
        , nr_subq_11.customer_third_hop_id__third_hop_ds__extract_quarter
        , nr_subq_11.customer_third_hop_id__third_hop_ds__extract_month
        , nr_subq_11.customer_third_hop_id__third_hop_ds__extract_day
        , nr_subq_11.customer_third_hop_id__third_hop_ds__extract_dow
        , nr_subq_11.customer_third_hop_id__third_hop_ds__extract_doy
        , nr_subq_11.metric_time__day
        , nr_subq_11.metric_time__week
        , nr_subq_11.metric_time__month
        , nr_subq_11.metric_time__quarter
        , nr_subq_11.metric_time__year
        , nr_subq_11.metric_time__extract_year
        , nr_subq_11.metric_time__extract_quarter
        , nr_subq_11.metric_time__extract_month
        , nr_subq_11.metric_time__extract_day
        , nr_subq_11.metric_time__extract_dow
        , nr_subq_11.metric_time__extract_doy
        , nr_subq_11.customer_third_hop_id
        , nr_subq_11.customer_third_hop_id__customer_id__customer_third_hop_id
        , nr_subq_11.value
        , nr_subq_11.customer_third_hop_id__value
        , nr_subq_11.customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
        , nr_subq_11.third_hop_count
      FROM (
        -- Join Standard Outputs
        SELECT
          nr_subq_10.customer_id__customer_third_hop_id AS customer_third_hop_id__customer_id__customer_third_hop_id
          , nr_subq_10.customer_id__customer_third_hop_id__paraguayan_customers AS customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers
          , nr_subq_4.third_hop_ds__day AS third_hop_ds__day
          , nr_subq_4.third_hop_ds__week AS third_hop_ds__week
          , nr_subq_4.third_hop_ds__month AS third_hop_ds__month
          , nr_subq_4.third_hop_ds__quarter AS third_hop_ds__quarter
          , nr_subq_4.third_hop_ds__year AS third_hop_ds__year
          , nr_subq_4.third_hop_ds__extract_year AS third_hop_ds__extract_year
          , nr_subq_4.third_hop_ds__extract_quarter AS third_hop_ds__extract_quarter
          , nr_subq_4.third_hop_ds__extract_month AS third_hop_ds__extract_month
          , nr_subq_4.third_hop_ds__extract_day AS third_hop_ds__extract_day
          , nr_subq_4.third_hop_ds__extract_dow AS third_hop_ds__extract_dow
          , nr_subq_4.third_hop_ds__extract_doy AS third_hop_ds__extract_doy
          , nr_subq_4.customer_third_hop_id__third_hop_ds__day AS customer_third_hop_id__third_hop_ds__day
          , nr_subq_4.customer_third_hop_id__third_hop_ds__week AS customer_third_hop_id__third_hop_ds__week
          , nr_subq_4.customer_third_hop_id__third_hop_ds__month AS customer_third_hop_id__third_hop_ds__month
          , nr_subq_4.customer_third_hop_id__third_hop_ds__quarter AS customer_third_hop_id__third_hop_ds__quarter
          , nr_subq_4.customer_third_hop_id__third_hop_ds__year AS customer_third_hop_id__third_hop_ds__year
          , nr_subq_4.customer_third_hop_id__third_hop_ds__extract_year AS customer_third_hop_id__third_hop_ds__extract_year
          , nr_subq_4.customer_third_hop_id__third_hop_ds__extract_quarter AS customer_third_hop_id__third_hop_ds__extract_quarter
          , nr_subq_4.customer_third_hop_id__third_hop_ds__extract_month AS customer_third_hop_id__third_hop_ds__extract_month
          , nr_subq_4.customer_third_hop_id__third_hop_ds__extract_day AS customer_third_hop_id__third_hop_ds__extract_day
          , nr_subq_4.customer_third_hop_id__third_hop_ds__extract_dow AS customer_third_hop_id__third_hop_ds__extract_dow
          , nr_subq_4.customer_third_hop_id__third_hop_ds__extract_doy AS customer_third_hop_id__third_hop_ds__extract_doy
          , nr_subq_4.metric_time__day AS metric_time__day
          , nr_subq_4.metric_time__week AS metric_time__week
          , nr_subq_4.metric_time__month AS metric_time__month
          , nr_subq_4.metric_time__quarter AS metric_time__quarter
          , nr_subq_4.metric_time__year AS metric_time__year
          , nr_subq_4.metric_time__extract_year AS metric_time__extract_year
          , nr_subq_4.metric_time__extract_quarter AS metric_time__extract_quarter
          , nr_subq_4.metric_time__extract_month AS metric_time__extract_month
          , nr_subq_4.metric_time__extract_day AS metric_time__extract_day
          , nr_subq_4.metric_time__extract_dow AS metric_time__extract_dow
          , nr_subq_4.metric_time__extract_doy AS metric_time__extract_doy
          , nr_subq_4.customer_third_hop_id AS customer_third_hop_id
          , nr_subq_4.value AS value
          , nr_subq_4.customer_third_hop_id__value AS customer_third_hop_id__value
          , nr_subq_4.third_hop_count AS third_hop_count
        FROM (
          -- Metric Time Dimension 'third_hop_ds'
          SELECT
            nr_subq_22004.third_hop_ds__day
            , nr_subq_22004.third_hop_ds__week
            , nr_subq_22004.third_hop_ds__month
            , nr_subq_22004.third_hop_ds__quarter
            , nr_subq_22004.third_hop_ds__year
            , nr_subq_22004.third_hop_ds__extract_year
            , nr_subq_22004.third_hop_ds__extract_quarter
            , nr_subq_22004.third_hop_ds__extract_month
            , nr_subq_22004.third_hop_ds__extract_day
            , nr_subq_22004.third_hop_ds__extract_dow
            , nr_subq_22004.third_hop_ds__extract_doy
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__day
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__week
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__month
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__quarter
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__year
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__extract_year
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__extract_quarter
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__extract_month
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__extract_day
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__extract_dow
            , nr_subq_22004.customer_third_hop_id__third_hop_ds__extract_doy
            , nr_subq_22004.third_hop_ds__day AS metric_time__day
            , nr_subq_22004.third_hop_ds__week AS metric_time__week
            , nr_subq_22004.third_hop_ds__month AS metric_time__month
            , nr_subq_22004.third_hop_ds__quarter AS metric_time__quarter
            , nr_subq_22004.third_hop_ds__year AS metric_time__year
            , nr_subq_22004.third_hop_ds__extract_year AS metric_time__extract_year
            , nr_subq_22004.third_hop_ds__extract_quarter AS metric_time__extract_quarter
            , nr_subq_22004.third_hop_ds__extract_month AS metric_time__extract_month
            , nr_subq_22004.third_hop_ds__extract_day AS metric_time__extract_day
            , nr_subq_22004.third_hop_ds__extract_dow AS metric_time__extract_dow
            , nr_subq_22004.third_hop_ds__extract_doy AS metric_time__extract_doy
            , nr_subq_22004.customer_third_hop_id
            , nr_subq_22004.value
            , nr_subq_22004.customer_third_hop_id__value
            , nr_subq_22004.third_hop_count
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
          ) nr_subq_22004
        ) nr_subq_4
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['customer_id__customer_third_hop_id', 'customer_id__customer_third_hop_id__paraguayan_customers']
          SELECT
            nr_subq_9.customer_id__customer_third_hop_id
            , nr_subq_9.customer_id__customer_third_hop_id__paraguayan_customers
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              nr_subq_8.customer_id__customer_third_hop_id
              , nr_subq_8.customers_with_other_data AS customer_id__customer_third_hop_id__paraguayan_customers
            FROM (
              -- Aggregate Measures
              SELECT
                nr_subq_7.customer_id__customer_third_hop_id
                , SUM(nr_subq_7.customers_with_other_data) AS customers_with_other_data
              FROM (
                -- Pass Only Elements: ['customers_with_other_data', 'customer_id__customer_third_hop_id']
                SELECT
                  nr_subq_6.customer_id__customer_third_hop_id
                  , nr_subq_6.customers_with_other_data
                FROM (
                  -- Constrain Output with WHERE
                  SELECT
                    nr_subq_5.acquired_ds__day
                    , nr_subq_5.acquired_ds__week
                    , nr_subq_5.acquired_ds__month
                    , nr_subq_5.acquired_ds__quarter
                    , nr_subq_5.acquired_ds__year
                    , nr_subq_5.acquired_ds__extract_year
                    , nr_subq_5.acquired_ds__extract_quarter
                    , nr_subq_5.acquired_ds__extract_month
                    , nr_subq_5.acquired_ds__extract_day
                    , nr_subq_5.acquired_ds__extract_dow
                    , nr_subq_5.acquired_ds__extract_doy
                    , nr_subq_5.customer_id__acquired_ds__day
                    , nr_subq_5.customer_id__acquired_ds__week
                    , nr_subq_5.customer_id__acquired_ds__month
                    , nr_subq_5.customer_id__acquired_ds__quarter
                    , nr_subq_5.customer_id__acquired_ds__year
                    , nr_subq_5.customer_id__acquired_ds__extract_year
                    , nr_subq_5.customer_id__acquired_ds__extract_quarter
                    , nr_subq_5.customer_id__acquired_ds__extract_month
                    , nr_subq_5.customer_id__acquired_ds__extract_day
                    , nr_subq_5.customer_id__acquired_ds__extract_dow
                    , nr_subq_5.customer_id__acquired_ds__extract_doy
                    , nr_subq_5.customer_third_hop_id__acquired_ds__day
                    , nr_subq_5.customer_third_hop_id__acquired_ds__week
                    , nr_subq_5.customer_third_hop_id__acquired_ds__month
                    , nr_subq_5.customer_third_hop_id__acquired_ds__quarter
                    , nr_subq_5.customer_third_hop_id__acquired_ds__year
                    , nr_subq_5.customer_third_hop_id__acquired_ds__extract_year
                    , nr_subq_5.customer_third_hop_id__acquired_ds__extract_quarter
                    , nr_subq_5.customer_third_hop_id__acquired_ds__extract_month
                    , nr_subq_5.customer_third_hop_id__acquired_ds__extract_day
                    , nr_subq_5.customer_third_hop_id__acquired_ds__extract_dow
                    , nr_subq_5.customer_third_hop_id__acquired_ds__extract_doy
                    , nr_subq_5.metric_time__day
                    , nr_subq_5.metric_time__week
                    , nr_subq_5.metric_time__month
                    , nr_subq_5.metric_time__quarter
                    , nr_subq_5.metric_time__year
                    , nr_subq_5.metric_time__extract_year
                    , nr_subq_5.metric_time__extract_quarter
                    , nr_subq_5.metric_time__extract_month
                    , nr_subq_5.metric_time__extract_day
                    , nr_subq_5.metric_time__extract_dow
                    , nr_subq_5.metric_time__extract_doy
                    , nr_subq_5.customer_id
                    , nr_subq_5.customer_third_hop_id
                    , nr_subq_5.customer_id__customer_third_hop_id
                    , nr_subq_5.customer_third_hop_id__customer_id
                    , nr_subq_5.country
                    , nr_subq_5.customer_id__country
                    , nr_subq_5.customer_third_hop_id__country
                    , nr_subq_5.customers_with_other_data
                  FROM (
                    -- Metric Time Dimension 'acquired_ds'
                    SELECT
                      nr_subq_22002.acquired_ds__day
                      , nr_subq_22002.acquired_ds__week
                      , nr_subq_22002.acquired_ds__month
                      , nr_subq_22002.acquired_ds__quarter
                      , nr_subq_22002.acquired_ds__year
                      , nr_subq_22002.acquired_ds__extract_year
                      , nr_subq_22002.acquired_ds__extract_quarter
                      , nr_subq_22002.acquired_ds__extract_month
                      , nr_subq_22002.acquired_ds__extract_day
                      , nr_subq_22002.acquired_ds__extract_dow
                      , nr_subq_22002.acquired_ds__extract_doy
                      , nr_subq_22002.customer_id__acquired_ds__day
                      , nr_subq_22002.customer_id__acquired_ds__week
                      , nr_subq_22002.customer_id__acquired_ds__month
                      , nr_subq_22002.customer_id__acquired_ds__quarter
                      , nr_subq_22002.customer_id__acquired_ds__year
                      , nr_subq_22002.customer_id__acquired_ds__extract_year
                      , nr_subq_22002.customer_id__acquired_ds__extract_quarter
                      , nr_subq_22002.customer_id__acquired_ds__extract_month
                      , nr_subq_22002.customer_id__acquired_ds__extract_day
                      , nr_subq_22002.customer_id__acquired_ds__extract_dow
                      , nr_subq_22002.customer_id__acquired_ds__extract_doy
                      , nr_subq_22002.customer_third_hop_id__acquired_ds__day
                      , nr_subq_22002.customer_third_hop_id__acquired_ds__week
                      , nr_subq_22002.customer_third_hop_id__acquired_ds__month
                      , nr_subq_22002.customer_third_hop_id__acquired_ds__quarter
                      , nr_subq_22002.customer_third_hop_id__acquired_ds__year
                      , nr_subq_22002.customer_third_hop_id__acquired_ds__extract_year
                      , nr_subq_22002.customer_third_hop_id__acquired_ds__extract_quarter
                      , nr_subq_22002.customer_third_hop_id__acquired_ds__extract_month
                      , nr_subq_22002.customer_third_hop_id__acquired_ds__extract_day
                      , nr_subq_22002.customer_third_hop_id__acquired_ds__extract_dow
                      , nr_subq_22002.customer_third_hop_id__acquired_ds__extract_doy
                      , nr_subq_22002.acquired_ds__day AS metric_time__day
                      , nr_subq_22002.acquired_ds__week AS metric_time__week
                      , nr_subq_22002.acquired_ds__month AS metric_time__month
                      , nr_subq_22002.acquired_ds__quarter AS metric_time__quarter
                      , nr_subq_22002.acquired_ds__year AS metric_time__year
                      , nr_subq_22002.acquired_ds__extract_year AS metric_time__extract_year
                      , nr_subq_22002.acquired_ds__extract_quarter AS metric_time__extract_quarter
                      , nr_subq_22002.acquired_ds__extract_month AS metric_time__extract_month
                      , nr_subq_22002.acquired_ds__extract_day AS metric_time__extract_day
                      , nr_subq_22002.acquired_ds__extract_dow AS metric_time__extract_dow
                      , nr_subq_22002.acquired_ds__extract_doy AS metric_time__extract_doy
                      , nr_subq_22002.customer_id
                      , nr_subq_22002.customer_third_hop_id
                      , nr_subq_22002.customer_id__customer_third_hop_id
                      , nr_subq_22002.customer_third_hop_id__customer_id
                      , nr_subq_22002.country
                      , nr_subq_22002.customer_id__country
                      , nr_subq_22002.customer_third_hop_id__country
                      , nr_subq_22002.customers_with_other_data
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
                    ) nr_subq_22002
                  ) nr_subq_5
                  WHERE customer_id__country = 'paraguay'
                ) nr_subq_6
              ) nr_subq_7
              GROUP BY
                nr_subq_7.customer_id__customer_third_hop_id
            ) nr_subq_8
          ) nr_subq_9
        ) nr_subq_10
        ON
          nr_subq_4.customer_third_hop_id = nr_subq_10.customer_id__customer_third_hop_id
      ) nr_subq_11
      WHERE customer_third_hop_id__customer_id__customer_third_hop_id__paraguayan_customers > 0
    ) nr_subq_12
  ) nr_subq_13
) nr_subq_14
