WITH house_avg AS (
  SELECT
    municipality_name_fr,
    district_name_fr AS district,
    AVG(median_price) AS avg_median_price,
    ROW_NUMBER() OVER (PARTITION BY district_name_fr ORDER BY AVG(median_price) DESC) AS rank
  FROM int_transaction
  WHERE property_type = 'house_all' AND n_transactions >= 50
  GROUP BY municipality_name_fr, district_name_fr
)
SELECT
  district,
  municipality_name_fr,
  ROUND(avg_median_price::numeric, 0) AS avg_median_price,
  rank
FROM house_avg
WHERE rank <= 3
ORDER BY district, rank;