-- Read Elements From Semantic Model 'bookings_source'
-- Metric Time Dimension 'paid_at'
SELECT
  ds
  , DATE_TRUNC('week', ds) AS ds__week
  , DATE_TRUNC('month', ds) AS ds__month
  , DATE_TRUNC('quarter', ds) AS ds__quarter
  , DATE_TRUNC('year', ds) AS ds__year
  , ds_partitioned
  , DATE_TRUNC('week', ds_partitioned) AS ds_partitioned__week
  , DATE_TRUNC('month', ds_partitioned) AS ds_partitioned__month
  , DATE_TRUNC('quarter', ds_partitioned) AS ds_partitioned__quarter
  , DATE_TRUNC('year', ds_partitioned) AS ds_partitioned__year
  , paid_at
  , DATE_TRUNC('week', paid_at) AS paid_at__week
  , DATE_TRUNC('month', paid_at) AS paid_at__month
  , DATE_TRUNC('quarter', paid_at) AS paid_at__quarter
  , DATE_TRUNC('year', paid_at) AS paid_at__year
  , paid_at AS metric_time
  , DATE_TRUNC('week', paid_at) AS metric_time__week
  , DATE_TRUNC('month', paid_at) AS metric_time__month
  , DATE_TRUNC('quarter', paid_at) AS metric_time__quarter
  , DATE_TRUNC('year', paid_at) AS metric_time__year
  , listing_id AS listing
  , guest_id AS guest
  , host_id AS host
  , is_instant
  , booking_value AS booking_payments
FROM ***************************.fct_bookings bookings_source_src_10001
