# Belgium Real Estate ETL Pipeline 🚀

Containerized ETL pipeline for Statbel quarterly real estate data. 

**No local Python or PostgreSQL required**—runs anywhere with Docker.

## Features
- Downloads Statbel real estate Excel files automatically
- Parses 142k+ transaction records across 2.7k municipalities
- Joins with REFNIS geographic hierarchy (region → province → district → municipality)
- Creates clean `int_transaction` table with median prices by property type
- SQL-ready for price analysis by district/municipality

## Prerequisites

Docker + Docker Compose only

No Python, pip, PostgreSQL, or Excel install needed


## Quick Start
```bash
git clone https://github.com/rcariat/belgium-real-estate.git
cd belgium-real-estate
docker compose up --build
```

**~3 minutes first run**:
1. Downloads Excel data to `./data/`
2. Creates `stg_transaction`, `stg_refnis`, `int_transaction` tables

## Verify Results
```bash
# List tables
docker compose exec postgres psql -U postgres -d real_estate -c "\dt"

# Check data
docker compose exec postgres psql -U postgres -d real_estate -c "SELECT COUNT(*) FROM int_transaction;"

# Sample analysis
docker compose exec postgres psql -U postgres -d real_estate -c "
SELECT district_name_fr, municipality_name_fr, ROUND(AVG(median_price)::numeric,0) 
FROM int_transaction 
WHERE property_type = 'house_all' AND n_transactions >= 50
GROUP BY 1,2 ORDER BY 3 DESC LIMIT 10;
"
```

## Data flow
1. Statbel Excel → `stg_transaction` (raw)
2. REFNIS Excel → `stg_refnis` (geo lookup)  
3. SQL `CREATE TABLE AS` → `int_transaction` (silver/joined)

## Key Queries

The `sql/` folder contains 6 queries:


1. **Missing Data %** : % of periods with `n_transactions NULL` or `< 50` by property type + district
2. **House Price Ranking** : Top 3 municipalities per district by avg median house price (`house_all`, ≥50 transactions)
3. **Intra-Year Variation** : Municipalities with greatest quarterly price swings within each calendar year (CV = stddev/mean)
4. **Price Gap from District** : Municipality furthest from its district median (1 per district)
5. **Interquartile Range** : Widest price spreads (avg Q3-Q1 gap, houses only) 
6. **Sales Share** : Top 3 municipalities by % of district sales volume

**Run any query:**
```bash
docker compose exec postgres psql -U postgres -d real_estate -f sql/query_name.sql
```

## Quarterly Updates if needed (Cron)
```bash
# Host cron (1st day of each quarter)
0 0 1 1,4,7,10 * cd /path/to/repo && docker compose run --rm etl >> etl.log 2>&1
```

## Infrastructure as Code

✅ No local dependencies (Docker only)

✅ Reproducible environment

✅ Automatic DB provisioning

✅ Portable (Windows/Mac/Linux)

✅ GitHub Actions ready

## Files
| File                 | Purpose                      |
|----------------------|------------------------------|
| `Dockerfile`         | Python 3.11 + ETL deps       |
| `docker-compose.yml` | Postgres + ETL orchestration |
| `etl_pipeline.py`    | Statbel → PostgreSQL ETL     |
| `explore.ipynb`      | Exploratory notebook         |
| `requirements.txt`   | pandas, sqlalchemy, openpyxl |

## License
MIT