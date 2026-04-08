SELECT
  municipality_name_fr,
  district_name_fr,
  ROUND(AVG(price_q3 - price_q1)::numeric, 0) AS avg_iqr
FROM int_transaction
WHERE property_type = 'house_all' AND n_transactions >= 50
GROUP BY municipality_name_fr, district_name_fr
ORDER BY avg_iqr DESC
LIMIT 10;