-- Read Elements From Semantic Model 'id_verifications'
SELECT
  1 AS identity_verifications
  , DATE_TRUNC(id_verifications_src_10003.ds, day) AS ds__day
  , DATE_TRUNC(id_verifications_src_10003.ds, isoweek) AS ds__week
  , DATE_TRUNC(id_verifications_src_10003.ds, month) AS ds__month
  , DATE_TRUNC(id_verifications_src_10003.ds, quarter) AS ds__quarter
  , DATE_TRUNC(id_verifications_src_10003.ds, year) AS ds__year
  , EXTRACT(year FROM id_verifications_src_10003.ds) AS ds__extract_year
  , EXTRACT(quarter FROM id_verifications_src_10003.ds) AS ds__extract_quarter
  , EXTRACT(month FROM id_verifications_src_10003.ds) AS ds__extract_month
  , EXTRACT(isoweek FROM id_verifications_src_10003.ds) AS ds__extract_week
  , EXTRACT(day FROM id_verifications_src_10003.ds) AS ds__extract_day
  , EXTRACT(dayofweek FROM id_verifications_src_10003.ds) AS ds__extract_dow
  , EXTRACT(dayofyear FROM id_verifications_src_10003.ds) AS ds__extract_doy
  , DATE_TRUNC(id_verifications_src_10003.ds_partitioned, day) AS ds_partitioned__day
  , DATE_TRUNC(id_verifications_src_10003.ds_partitioned, isoweek) AS ds_partitioned__week
  , DATE_TRUNC(id_verifications_src_10003.ds_partitioned, month) AS ds_partitioned__month
  , DATE_TRUNC(id_verifications_src_10003.ds_partitioned, quarter) AS ds_partitioned__quarter
  , DATE_TRUNC(id_verifications_src_10003.ds_partitioned, year) AS ds_partitioned__year
  , EXTRACT(year FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_year
  , EXTRACT(quarter FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_quarter
  , EXTRACT(month FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_month
  , EXTRACT(isoweek FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_week
  , EXTRACT(day FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_day
  , EXTRACT(dayofweek FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_dow
  , EXTRACT(dayofyear FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_doy
  , id_verifications_src_10003.verification_type
  , DATE_TRUNC(id_verifications_src_10003.ds, day) AS verification__ds__day
  , DATE_TRUNC(id_verifications_src_10003.ds, isoweek) AS verification__ds__week
  , DATE_TRUNC(id_verifications_src_10003.ds, month) AS verification__ds__month
  , DATE_TRUNC(id_verifications_src_10003.ds, quarter) AS verification__ds__quarter
  , DATE_TRUNC(id_verifications_src_10003.ds, year) AS verification__ds__year
  , EXTRACT(year FROM id_verifications_src_10003.ds) AS verification__ds__extract_year
  , EXTRACT(quarter FROM id_verifications_src_10003.ds) AS verification__ds__extract_quarter
  , EXTRACT(month FROM id_verifications_src_10003.ds) AS verification__ds__extract_month
  , EXTRACT(isoweek FROM id_verifications_src_10003.ds) AS verification__ds__extract_week
  , EXTRACT(day FROM id_verifications_src_10003.ds) AS verification__ds__extract_day
  , EXTRACT(dayofweek FROM id_verifications_src_10003.ds) AS verification__ds__extract_dow
  , EXTRACT(dayofyear FROM id_verifications_src_10003.ds) AS verification__ds__extract_doy
  , DATE_TRUNC(id_verifications_src_10003.ds_partitioned, day) AS verification__ds_partitioned__day
  , DATE_TRUNC(id_verifications_src_10003.ds_partitioned, isoweek) AS verification__ds_partitioned__week
  , DATE_TRUNC(id_verifications_src_10003.ds_partitioned, month) AS verification__ds_partitioned__month
  , DATE_TRUNC(id_verifications_src_10003.ds_partitioned, quarter) AS verification__ds_partitioned__quarter
  , DATE_TRUNC(id_verifications_src_10003.ds_partitioned, year) AS verification__ds_partitioned__year
  , EXTRACT(year FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_year
  , EXTRACT(quarter FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_quarter
  , EXTRACT(month FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_month
  , EXTRACT(isoweek FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_week
  , EXTRACT(day FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_day
  , EXTRACT(dayofweek FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_dow
  , EXTRACT(dayofyear FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_doy
  , id_verifications_src_10003.verification_type AS verification__verification_type
  , id_verifications_src_10003.verification_id AS verification
  , id_verifications_src_10003.user_id AS user
  , id_verifications_src_10003.user_id AS verification__user
FROM ***************************.fct_id_verifications id_verifications_src_10003
