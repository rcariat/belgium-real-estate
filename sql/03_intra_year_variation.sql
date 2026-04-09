WITH house_var AS (
  SELECT
    municipality_name_fr,
    district_name_fr,
    STDDEV(median_price) / AVG(median_price) AS cv_ratio
  FROM int_transaction
  WHERE property_type = 'house_all' AND n_transactions >= 50
  GROUP BY municipality_name_fr, district_name_fr, year
  HAVING COUNT(*) >= 2  -- Need 2+ periods/year for variation
)
SELECT
  municipality_name_fr,
  district_name_fr,
  ROUND(AVG(cv_ratio)::numeric * 100, 2) AS avg_cv_pct
FROM house_var
WHERE cv_ratio is not null
GROUP BY municipality_name_fr, district_name_fr
ORDER BY avg_cv_pct DESC;