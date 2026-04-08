WITH district_totals AS (
  SELECT
    district_name_fr,
    SUM(n_transactions) AS district_total_sales
  FROM int_transaction
  WHERE property_type = 'house_all'
  GROUP BY district_name_fr
),
muni_sales AS (
  SELECT
    it.municipality_name_fr,
    it.district_name_fr,
    SUM(it.n_transactions) AS muni_sales,
    dt.district_total_sales,
    ROW_NUMBER() OVER (
      PARTITION BY it.district_name_fr
      ORDER BY SUM(it.n_transactions) DESC
    ) AS rank
  FROM int_transaction it
  JOIN district_totals dt ON it.district_name_fr = dt.district_name_fr
  WHERE it.property_type = 'house_all'
  GROUP BY it.municipality_name_fr, it.district_name_fr, dt.district_total_sales
)
SELECT
  district_name_fr AS district,
  municipality_name_fr,
  muni_sales,
  district_total_sales,
  ROUND((100.0 * muni_sales / district_total_sales)::numeric, 2) AS sales_pct
FROM muni_sales
WHERE rank <= 3
ORDER BY district, rank;