-- Read Elements From Semantic Model 'id_verifications'
SELECT
  1 AS identity_verifications
  , DATE_TRUNC(id_verifications_src_10004.ds, day) AS ds__day
  , DATE_TRUNC(id_verifications_src_10004.ds, isoweek) AS ds__week
  , DATE_TRUNC(id_verifications_src_10004.ds, month) AS ds__month
  , DATE_TRUNC(id_verifications_src_10004.ds, quarter) AS ds__quarter
  , DATE_TRUNC(id_verifications_src_10004.ds, year) AS ds__year
  , EXTRACT(year FROM id_verifications_src_10004.ds) AS ds__extract_year
  , EXTRACT(quarter FROM id_verifications_src_10004.ds) AS ds__extract_quarter
  , EXTRACT(month FROM id_verifications_src_10004.ds) AS ds__extract_month
  , EXTRACT(day FROM id_verifications_src_10004.ds) AS ds__extract_day
  , IF(EXTRACT(dayofweek FROM id_verifications_src_10004.ds) = 1, 7, EXTRACT(dayofweek FROM id_verifications_src_10004.ds) - 1) AS ds__extract_dow
  , EXTRACT(dayofyear FROM id_verifications_src_10004.ds) AS ds__extract_doy
  , DATE_TRUNC(id_verifications_src_10004.ds_partitioned, day) AS ds_partitioned__day
  , DATE_TRUNC(id_verifications_src_10004.ds_partitioned, isoweek) AS ds_partitioned__week
  , DATE_TRUNC(id_verifications_src_10004.ds_partitioned, month) AS ds_partitioned__month
  , DATE_TRUNC(id_verifications_src_10004.ds_partitioned, quarter) AS ds_partitioned__quarter
  , DATE_TRUNC(id_verifications_src_10004.ds_partitioned, year) AS ds_partitioned__year
  , EXTRACT(year FROM id_verifications_src_10004.ds_partitioned) AS ds_partitioned__extract_year
  , EXTRACT(quarter FROM id_verifications_src_10004.ds_partitioned) AS ds_partitioned__extract_quarter
  , EXTRACT(month FROM id_verifications_src_10004.ds_partitioned) AS ds_partitioned__extract_month
  , EXTRACT(day FROM id_verifications_src_10004.ds_partitioned) AS ds_partitioned__extract_day
  , IF(EXTRACT(dayofweek FROM id_verifications_src_10004.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM id_verifications_src_10004.ds_partitioned) - 1) AS ds_partitioned__extract_dow
  , EXTRACT(dayofyear FROM id_verifications_src_10004.ds_partitioned) AS ds_partitioned__extract_doy
  , id_verifications_src_10004.verification_type
  , DATE_TRUNC(id_verifications_src_10004.ds, day) AS verification__ds__day
  , DATE_TRUNC(id_verifications_src_10004.ds, isoweek) AS verification__ds__week
  , DATE_TRUNC(id_verifications_src_10004.ds, month) AS verification__ds__month
  , DATE_TRUNC(id_verifications_src_10004.ds, quarter) AS verification__ds__quarter
  , DATE_TRUNC(id_verifications_src_10004.ds, year) AS verification__ds__year
  , EXTRACT(year FROM id_verifications_src_10004.ds) AS verification__ds__extract_year
  , EXTRACT(quarter FROM id_verifications_src_10004.ds) AS verification__ds__extract_quarter
  , EXTRACT(month FROM id_verifications_src_10004.ds) AS verification__ds__extract_month
  , EXTRACT(day FROM id_verifications_src_10004.ds) AS verification__ds__extract_day
  , IF(EXTRACT(dayofweek FROM id_verifications_src_10004.ds) = 1, 7, EXTRACT(dayofweek FROM id_verifications_src_10004.ds) - 1) AS verification__ds__extract_dow
  , EXTRACT(dayofyear FROM id_verifications_src_10004.ds) AS verification__ds__extract_doy
  , DATE_TRUNC(id_verifications_src_10004.ds_partitioned, day) AS verification__ds_partitioned__day
  , DATE_TRUNC(id_verifications_src_10004.ds_partitioned, isoweek) AS verification__ds_partitioned__week
  , DATE_TRUNC(id_verifications_src_10004.ds_partitioned, month) AS verification__ds_partitioned__month
  , DATE_TRUNC(id_verifications_src_10004.ds_partitioned, quarter) AS verification__ds_partitioned__quarter
  , DATE_TRUNC(id_verifications_src_10004.ds_partitioned, year) AS verification__ds_partitioned__year
  , EXTRACT(year FROM id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__extract_year
  , EXTRACT(quarter FROM id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__extract_quarter
  , EXTRACT(month FROM id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__extract_month
  , EXTRACT(day FROM id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__extract_day
  , IF(EXTRACT(dayofweek FROM id_verifications_src_10004.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM id_verifications_src_10004.ds_partitioned) - 1) AS verification__ds_partitioned__extract_dow
  , EXTRACT(dayofyear FROM id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__extract_doy
  , id_verifications_src_10004.verification_type AS verification__verification_type
  , id_verifications_src_10004.verification_id AS verification
  , id_verifications_src_10004.user_id AS user
  , id_verifications_src_10004.user_id AS verification__user
FROM ***************************.fct_id_verifications id_verifications_src_10004
