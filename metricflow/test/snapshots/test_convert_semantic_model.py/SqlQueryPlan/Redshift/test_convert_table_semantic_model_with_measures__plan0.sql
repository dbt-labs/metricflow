-- Read Elements From Semantic Model 'id_verifications'
SELECT
  1 AS identity_verifications
  , DATE_TRUNC('day', id_verifications_src_10004.ds) AS ds__day
  , DATE_TRUNC('week', id_verifications_src_10004.ds) AS ds__week
  , DATE_TRUNC('month', id_verifications_src_10004.ds) AS ds__month
  , DATE_TRUNC('quarter', id_verifications_src_10004.ds) AS ds__quarter
  , DATE_TRUNC('year', id_verifications_src_10004.ds) AS ds__year
  , EXTRACT(year FROM id_verifications_src_10004.ds) AS ds__extract_year
  , EXTRACT(quarter FROM id_verifications_src_10004.ds) AS ds__extract_quarter
  , EXTRACT(month FROM id_verifications_src_10004.ds) AS ds__extract_month
  , EXTRACT(day FROM id_verifications_src_10004.ds) AS ds__extract_day
  , CASE WHEN EXTRACT(dow FROM id_verifications_src_10004.ds) = 0 THEN EXTRACT(dow FROM id_verifications_src_10004.ds) + 7 ELSE EXTRACT(dow FROM id_verifications_src_10004.ds) END AS ds__extract_dow
  , EXTRACT(doy FROM id_verifications_src_10004.ds) AS ds__extract_doy
  , DATE_TRUNC('day', id_verifications_src_10004.ds_partitioned) AS ds_partitioned__day
  , DATE_TRUNC('week', id_verifications_src_10004.ds_partitioned) AS ds_partitioned__week
  , DATE_TRUNC('month', id_verifications_src_10004.ds_partitioned) AS ds_partitioned__month
  , DATE_TRUNC('quarter', id_verifications_src_10004.ds_partitioned) AS ds_partitioned__quarter
  , DATE_TRUNC('year', id_verifications_src_10004.ds_partitioned) AS ds_partitioned__year
  , EXTRACT(year FROM id_verifications_src_10004.ds_partitioned) AS ds_partitioned__extract_year
  , EXTRACT(quarter FROM id_verifications_src_10004.ds_partitioned) AS ds_partitioned__extract_quarter
  , EXTRACT(month FROM id_verifications_src_10004.ds_partitioned) AS ds_partitioned__extract_month
  , EXTRACT(day FROM id_verifications_src_10004.ds_partitioned) AS ds_partitioned__extract_day
  , CASE WHEN EXTRACT(dow FROM id_verifications_src_10004.ds_partitioned) = 0 THEN EXTRACT(dow FROM id_verifications_src_10004.ds_partitioned) + 7 ELSE EXTRACT(dow FROM id_verifications_src_10004.ds_partitioned) END AS ds_partitioned__extract_dow
  , EXTRACT(doy FROM id_verifications_src_10004.ds_partitioned) AS ds_partitioned__extract_doy
  , id_verifications_src_10004.verification_type
  , DATE_TRUNC('day', id_verifications_src_10004.ds) AS verification__ds__day
  , DATE_TRUNC('week', id_verifications_src_10004.ds) AS verification__ds__week
  , DATE_TRUNC('month', id_verifications_src_10004.ds) AS verification__ds__month
  , DATE_TRUNC('quarter', id_verifications_src_10004.ds) AS verification__ds__quarter
  , DATE_TRUNC('year', id_verifications_src_10004.ds) AS verification__ds__year
  , EXTRACT(year FROM id_verifications_src_10004.ds) AS verification__ds__extract_year
  , EXTRACT(quarter FROM id_verifications_src_10004.ds) AS verification__ds__extract_quarter
  , EXTRACT(month FROM id_verifications_src_10004.ds) AS verification__ds__extract_month
  , EXTRACT(day FROM id_verifications_src_10004.ds) AS verification__ds__extract_day
  , CASE WHEN EXTRACT(dow FROM id_verifications_src_10004.ds) = 0 THEN EXTRACT(dow FROM id_verifications_src_10004.ds) + 7 ELSE EXTRACT(dow FROM id_verifications_src_10004.ds) END AS verification__ds__extract_dow
  , EXTRACT(doy FROM id_verifications_src_10004.ds) AS verification__ds__extract_doy
  , DATE_TRUNC('day', id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__day
  , DATE_TRUNC('week', id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__week
  , DATE_TRUNC('month', id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__month
  , DATE_TRUNC('quarter', id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__quarter
  , DATE_TRUNC('year', id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__year
  , EXTRACT(year FROM id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__extract_year
  , EXTRACT(quarter FROM id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__extract_quarter
  , EXTRACT(month FROM id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__extract_month
  , EXTRACT(day FROM id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__extract_day
  , CASE WHEN EXTRACT(dow FROM id_verifications_src_10004.ds_partitioned) = 0 THEN EXTRACT(dow FROM id_verifications_src_10004.ds_partitioned) + 7 ELSE EXTRACT(dow FROM id_verifications_src_10004.ds_partitioned) END AS verification__ds_partitioned__extract_dow
  , EXTRACT(doy FROM id_verifications_src_10004.ds_partitioned) AS verification__ds_partitioned__extract_doy
  , id_verifications_src_10004.verification_type AS verification__verification_type
  , id_verifications_src_10004.verification_id AS verification
  , id_verifications_src_10004.user_id AS user
  , id_verifications_src_10004.user_id AS verification__user
FROM ***************************.fct_id_verifications id_verifications_src_10004
