WITH district_med AS (
  SELECT
    district_name_fr AS district,
    AVG(median_price) AS district_median
  FROM int_transaction
  WHERE property_type = 'house_all' AND n_transactions >= 50
  GROUP BY district_name_fr
),
muni_diff AS (
  SELECT
    it.municipality_name_fr,
    it.district_name_fr AS district,
    AVG(it.median_price) AS muni_median,
    dm.district_median,
    ABS(AVG(it.median_price) - dm.district_median) AS diff_abs
  FROM int_transaction it
  JOIN district_med dm ON it.district_name_fr = dm.district
  WHERE it.property_type = 'house_all' AND it.n_transactions >= 50
  GROUP BY it.municipality_name_fr, it.district_name_fr, dm.district_median
)
SELECT
  district,
  municipality_name_fr,
  ROUND(muni_median::numeric, 0) AS muni_median,
  ROUND(district_median::numeric, 0) AS district_median,
  ROUND(diff_abs::numeric, 0) AS diff_abs
FROM muni_diff
WHERE diff_abs = (
  SELECT MAX(diff_abs)
  FROM muni_diff md2
  WHERE md2.district = muni_diff.district
)
ORDER BY district;