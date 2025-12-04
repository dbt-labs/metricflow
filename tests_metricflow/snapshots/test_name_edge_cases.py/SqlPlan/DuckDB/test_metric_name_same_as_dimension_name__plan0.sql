test_name: test_metric_name_same_as_dimension_name
test_filename: test_name_edge_cases.py
docstring:
  Check a soon-to-be-deprecated use case where a manifest contains a metric with the same name as a dimension.
sql_engine: DuckDB
---
-- Write to DataTable
SELECT
  subq_5.booking__homonymous_metric_and_dimension
  , subq_5.homonymous_metric_and_dimension
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_4.booking__homonymous_metric_and_dimension
    , subq_4.__homonymous_metric_and_dimension AS homonymous_metric_and_dimension
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_3.booking__homonymous_metric_and_dimension
      , SUM(subq_3.__homonymous_metric_and_dimension) AS __homonymous_metric_and_dimension
    FROM (
      -- Pass Only Elements: ['__homonymous_metric_and_dimension', 'booking__homonymous_metric_and_dimension']
      SELECT
        subq_2.booking__homonymous_metric_and_dimension
        , subq_2.__homonymous_metric_and_dimension
      FROM (
        -- Pass Only Elements: ['__homonymous_metric_and_dimension', 'booking__homonymous_metric_and_dimension']
        SELECT
          subq_1.booking__homonymous_metric_and_dimension
          , subq_1.__homonymous_metric_and_dimension
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_0.ds__day
            , subq_0.ds__week
            , subq_0.ds__month
            , subq_0.ds__quarter
            , subq_0.ds__year
            , subq_0.ds__extract_year
            , subq_0.ds__extract_quarter
            , subq_0.ds__extract_month
            , subq_0.ds__extract_day
            , subq_0.ds__extract_dow
            , subq_0.ds__extract_doy
            , subq_0.booking__ds__day
            , subq_0.booking__ds__week
            , subq_0.booking__ds__month
            , subq_0.booking__ds__quarter
            , subq_0.booking__ds__year
            , subq_0.booking__ds__extract_year
            , subq_0.booking__ds__extract_quarter
            , subq_0.booking__ds__extract_month
            , subq_0.booking__ds__extract_day
            , subq_0.booking__ds__extract_dow
            , subq_0.booking__ds__extract_doy
            , subq_0.homonymous_metric_and_entity__ds__day
            , subq_0.homonymous_metric_and_entity__ds__week
            , subq_0.homonymous_metric_and_entity__ds__month
            , subq_0.homonymous_metric_and_entity__ds__quarter
            , subq_0.homonymous_metric_and_entity__ds__year
            , subq_0.homonymous_metric_and_entity__ds__extract_year
            , subq_0.homonymous_metric_and_entity__ds__extract_quarter
            , subq_0.homonymous_metric_and_entity__ds__extract_month
            , subq_0.homonymous_metric_and_entity__ds__extract_day
            , subq_0.homonymous_metric_and_entity__ds__extract_dow
            , subq_0.homonymous_metric_and_entity__ds__extract_doy
            , subq_0.ds__day AS metric_time__day
            , subq_0.ds__week AS metric_time__week
            , subq_0.ds__month AS metric_time__month
            , subq_0.ds__quarter AS metric_time__quarter
            , subq_0.ds__year AS metric_time__year
            , subq_0.ds__extract_year AS metric_time__extract_year
            , subq_0.ds__extract_quarter AS metric_time__extract_quarter
            , subq_0.ds__extract_month AS metric_time__extract_month
            , subq_0.ds__extract_day AS metric_time__extract_day
            , subq_0.ds__extract_dow AS metric_time__extract_dow
            , subq_0.ds__extract_doy AS metric_time__extract_doy
            , subq_0.homonymous_metric_and_entity
            , subq_0.booking__homonymous_metric_and_entity
            , subq_0.homonymous_metric_and_dimension
            , subq_0.booking__homonymous_metric_and_dimension
            , subq_0.homonymous_metric_and_entity__homonymous_metric_and_dimension
            , subq_0.__homonymous_metric_and_dimension
            , subq_0.__homonymous_metric_and_entity
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            SELECT
              bookings_source_src_32000.booking_value AS __homonymous_metric_and_dimension
              , bookings_source_src_32000.booking_value AS __homonymous_metric_and_entity
              , 'dummy_dimension_value' AS homonymous_metric_and_dimension
              , DATE_TRUNC('day', bookings_source_src_32000.ds) AS ds__day
              , DATE_TRUNC('week', bookings_source_src_32000.ds) AS ds__week
              , DATE_TRUNC('month', bookings_source_src_32000.ds) AS ds__month
              , DATE_TRUNC('quarter', bookings_source_src_32000.ds) AS ds__quarter
              , DATE_TRUNC('year', bookings_source_src_32000.ds) AS ds__year
              , EXTRACT(year FROM bookings_source_src_32000.ds) AS ds__extract_year
              , EXTRACT(quarter FROM bookings_source_src_32000.ds) AS ds__extract_quarter
              , EXTRACT(month FROM bookings_source_src_32000.ds) AS ds__extract_month
              , EXTRACT(day FROM bookings_source_src_32000.ds) AS ds__extract_day
              , EXTRACT(isodow FROM bookings_source_src_32000.ds) AS ds__extract_dow
              , EXTRACT(doy FROM bookings_source_src_32000.ds) AS ds__extract_doy
              , 'dummy_dimension_value' AS booking__homonymous_metric_and_dimension
              , DATE_TRUNC('day', bookings_source_src_32000.ds) AS booking__ds__day
              , DATE_TRUNC('week', bookings_source_src_32000.ds) AS booking__ds__week
              , DATE_TRUNC('month', bookings_source_src_32000.ds) AS booking__ds__month
              , DATE_TRUNC('quarter', bookings_source_src_32000.ds) AS booking__ds__quarter
              , DATE_TRUNC('year', bookings_source_src_32000.ds) AS booking__ds__year
              , EXTRACT(year FROM bookings_source_src_32000.ds) AS booking__ds__extract_year
              , EXTRACT(quarter FROM bookings_source_src_32000.ds) AS booking__ds__extract_quarter
              , EXTRACT(month FROM bookings_source_src_32000.ds) AS booking__ds__extract_month
              , EXTRACT(day FROM bookings_source_src_32000.ds) AS booking__ds__extract_day
              , EXTRACT(isodow FROM bookings_source_src_32000.ds) AS booking__ds__extract_dow
              , EXTRACT(doy FROM bookings_source_src_32000.ds) AS booking__ds__extract_doy
              , 'dummy_dimension_value' AS homonymous_metric_and_entity__homonymous_metric_and_dimension
              , DATE_TRUNC('day', bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__day
              , DATE_TRUNC('week', bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__week
              , DATE_TRUNC('month', bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__month
              , DATE_TRUNC('quarter', bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__quarter
              , DATE_TRUNC('year', bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__year
              , EXTRACT(year FROM bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__extract_year
              , EXTRACT(quarter FROM bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__extract_quarter
              , EXTRACT(month FROM bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__extract_month
              , EXTRACT(day FROM bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__extract_day
              , EXTRACT(isodow FROM bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__extract_dow
              , EXTRACT(doy FROM bookings_source_src_32000.ds) AS homonymous_metric_and_entity__ds__extract_doy
              , 'dummy_entity_value' AS homonymous_metric_and_entity
              , 'dummy_entity_value' AS booking__homonymous_metric_and_entity
            FROM ***************************.fct_bookings bookings_source_src_32000
          ) subq_0
        ) subq_1
      ) subq_2
    ) subq_3
    GROUP BY
      subq_3.booking__homonymous_metric_and_dimension
  ) subq_4
) subq_5
