DROP TABLE IF EXISTS marts_missing_pct;
CREATE TABLE marts_missing_pct AS
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

DROP TABLE IF EXISTS marts_house_avg;
CREATE TABLE marts_house_avg AS
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

DROP TABLE IF EXISTS marts_house_var;
CREATE TABLE marts_house_var AS
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
ORDER BY avg_cv_pct DESC
LIMIT 10;

DROP TABLE IF EXISTS marts_house_var;
CREATE TABLE marts_house_var AS
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
ORDER BY avg_cv_pct DESC
LIMIT 10;