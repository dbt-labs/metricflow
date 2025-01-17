test_name: test_convert_table_semantic_model_with_measures
test_filename: test_convert_semantic_model.py
docstring:
  Complete test of table semantic model conversion. This includes the full set of measures/entities/dimensions.

      Measures trigger a primary time dimension validation. Additionally, this includes both categorical and time
      dimension types, which should cover most, if not all, of the table source branches in the target class.
sql_engine: Clickhouse
---
-- Read Elements From Semantic Model 'id_verifications'
SELECT
  1 AS identity_verifications
  , date_trunc('day', id_verifications_src_28000.ds) AS ds__day
  , date_trunc('week', id_verifications_src_28000.ds) AS ds__week
  , date_trunc('month', id_verifications_src_28000.ds) AS ds__month
  , date_trunc('quarter', id_verifications_src_28000.ds) AS ds__quarter
  , date_trunc('year', id_verifications_src_28000.ds) AS ds__year
  , toYear(id_verifications_src_28000.ds) AS ds__extract_year
  , toQuarter(id_verifications_src_28000.ds) AS ds__extract_quarter
  , toMonth(id_verifications_src_28000.ds) AS ds__extract_month
  , toDayOfMonth(id_verifications_src_28000.ds) AS ds__extract_day
  , toDayOfWeek(id_verifications_src_28000.ds) AS ds__extract_dow
  , toDayOfYear(id_verifications_src_28000.ds) AS ds__extract_doy
  , date_trunc('day', id_verifications_src_28000.ds_partitioned) AS ds_partitioned__day
  , date_trunc('week', id_verifications_src_28000.ds_partitioned) AS ds_partitioned__week
  , date_trunc('month', id_verifications_src_28000.ds_partitioned) AS ds_partitioned__month
  , date_trunc('quarter', id_verifications_src_28000.ds_partitioned) AS ds_partitioned__quarter
  , date_trunc('year', id_verifications_src_28000.ds_partitioned) AS ds_partitioned__year
  , toYear(id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_year
  , toQuarter(id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
  , toMonth(id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_month
  , toDayOfMonth(id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_day
  , toDayOfWeek(id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
  , toDayOfYear(id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
  , id_verifications_src_28000.verification_type
  , date_trunc('day', id_verifications_src_28000.ds) AS verification__ds__day
  , date_trunc('week', id_verifications_src_28000.ds) AS verification__ds__week
  , date_trunc('month', id_verifications_src_28000.ds) AS verification__ds__month
  , date_trunc('quarter', id_verifications_src_28000.ds) AS verification__ds__quarter
  , date_trunc('year', id_verifications_src_28000.ds) AS verification__ds__year
  , toYear(id_verifications_src_28000.ds) AS verification__ds__extract_year
  , toQuarter(id_verifications_src_28000.ds) AS verification__ds__extract_quarter
  , toMonth(id_verifications_src_28000.ds) AS verification__ds__extract_month
  , toDayOfMonth(id_verifications_src_28000.ds) AS verification__ds__extract_day
  , toDayOfWeek(id_verifications_src_28000.ds) AS verification__ds__extract_dow
  , toDayOfYear(id_verifications_src_28000.ds) AS verification__ds__extract_doy
  , date_trunc('day', id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__day
  , date_trunc('week', id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__week
  , date_trunc('month', id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__month
  , date_trunc('quarter', id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__quarter
  , date_trunc('year', id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__year
  , toYear(id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_year
  , toQuarter(id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_quarter
  , toMonth(id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_month
  , toDayOfMonth(id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_day
  , toDayOfWeek(id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_dow
  , toDayOfYear(id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_doy
  , id_verifications_src_28000.verification_type AS verification__verification_type
  , id_verifications_src_28000.verification_id AS verification
  , id_verifications_src_28000.user_id AS user
  , id_verifications_src_28000.user_id AS verification__user
FROM ***************************.fct_id_verifications id_verifications_src_28000
