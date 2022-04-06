-- Read Elements From Data Source 'id_verifications'
SELECT
  1 AS identity_verifications
  , id_verifications_src_10002.ds
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
  , id_verifications_src_10002.ds_partitioned
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds_partitioned__year
  , id_verifications_src_10002.verification_type
  , id_verifications_src_10002.ds AS verification__ds
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS verification__ds__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS verification__ds__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS verification__ds__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS verification__ds__year
  , id_verifications_src_10002.ds_partitioned AS verification__ds_partitioned
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS verification__ds_partitioned__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS verification__ds_partitioned__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS verification__ds_partitioned__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS verification__ds_partitioned__year
  , id_verifications_src_10002.verification_type AS verification__verification_type
  , id_verifications_src_10002.verification_id AS verification
  , id_verifications_src_10002.user_id AS user
  , id_verifications_src_10002.user_id AS verification__user
FROM ***************************.fct_id_verifications id_verifications_src_10002
