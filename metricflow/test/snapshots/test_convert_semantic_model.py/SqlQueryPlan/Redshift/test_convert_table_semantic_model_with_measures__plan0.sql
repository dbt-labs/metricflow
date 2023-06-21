-- Read Elements From Semantic Model 'id_verifications'
SELECT
  1 AS identity_verifications
  , id_verifications_src_10003.ds
  , DATE_TRUNC('week', id_verifications_src_10003.ds) AS ds__week
  , DATE_TRUNC('month', id_verifications_src_10003.ds) AS ds__month
  , DATE_TRUNC('quarter', id_verifications_src_10003.ds) AS ds__quarter
  , DATE_TRUNC('year', id_verifications_src_10003.ds) AS ds__year
  , id_verifications_src_10003.ds_partitioned
  , DATE_TRUNC('week', id_verifications_src_10003.ds_partitioned) AS ds_partitioned__week
  , DATE_TRUNC('month', id_verifications_src_10003.ds_partitioned) AS ds_partitioned__month
  , DATE_TRUNC('quarter', id_verifications_src_10003.ds_partitioned) AS ds_partitioned__quarter
  , DATE_TRUNC('year', id_verifications_src_10003.ds_partitioned) AS ds_partitioned__year
  , id_verifications_src_10003.verification_type
  , id_verifications_src_10003.ds AS verification__ds
  , DATE_TRUNC('week', id_verifications_src_10003.ds) AS verification__ds__week
  , DATE_TRUNC('month', id_verifications_src_10003.ds) AS verification__ds__month
  , DATE_TRUNC('quarter', id_verifications_src_10003.ds) AS verification__ds__quarter
  , DATE_TRUNC('year', id_verifications_src_10003.ds) AS verification__ds__year
  , id_verifications_src_10003.ds_partitioned AS verification__ds_partitioned
  , DATE_TRUNC('week', id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__week
  , DATE_TRUNC('month', id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__month
  , DATE_TRUNC('quarter', id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__quarter
  , DATE_TRUNC('year', id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__year
  , id_verifications_src_10003.verification_type AS verification__verification_type
  , id_verifications_src_10003.verification_id AS verification
  , id_verifications_src_10003.user_id AS user
  , id_verifications_src_10003.user_id AS verification__user
FROM ***************************.fct_id_verifications id_verifications_src_10003
