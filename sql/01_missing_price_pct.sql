WITH data_quality AS (
  SELECT
    property_type,
    district_name_fr AS district,
    COUNT(*) AS total_periods,
    SUM(CASE WHEN n_transactions IS NULL THEN 1 ELSE 0 END) AS no_data,
  SUM(CASE WHEN n_transactions < 50 THEN 1 ELSE 0 END) AS insufficient_data
  FROM int_transaction
  GROUP BY property_type, district
)
SELECT
  property_type,
  district,
  total_periods,
  no_data,
  insufficient_data,
  no_data + insufficient_data as missing_count,
  ROUND(100.0 * (no_data + insufficient_data) / total_periods, 2) AS missing_percentage
FROM data_quality
ORDER BY missing_percentage ASC, property_type;