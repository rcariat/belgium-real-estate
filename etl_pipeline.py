#!/usr/bin/env python3
"""
Real Estate ETL Pipeline
==========================================
Downloads source files, loads/transforms data, writes to PostgreSQL.

Usage:
    python etl_pipeline.py [--db-url <postgresql_url>]

Example:
    python etl_pipeline.py --db-url "postgresql://postgres:your_pass@localhost:5432/real_estate"

"""

import argparse
import logging
import os
import sys

import pandas as pd
import requests
import sqlalchemy as sa
from sqlalchemy import text

# Configuration
REAL_ESTATE_URL = "https://statbel.fgov.be/sites/default/files/files/documents/Bouwen%20%26%20wonen/2.1%20Vastgoedprijzen/NM/FR_immo_statbel_trimestre_par_commune.xlsx"
REFNIS_URL = "https://statbel.fgov.be/sites/default/files/files/opendata/REFNIS%20code/TU_COM_REFNIS-20250101.xlsx"

STG_TRANSACTIONS_TABLE = "stg_transaction"
STG_REFNIS_TABLE = "stg_refnis"
INT_TRANSACTION_TABLE = "int_transaction"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def download_file(url: str, local_path: str) -> None:
    """Download file if not cached."""
    if os.path.exists(local_path):
        log.info(f"Using cached: {local_path}")
        return
    log.info(f"Downloading {url}...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(resp.content)
    log.info(f"Saved {len(resp.content):,} bytes")


def parse_real_estate(path: str) -> pd.DataFrame:
    """Parse Statbel Excel into tidy long format."""
    log.info(f"Parsing {path}")
    raw = pd.read_excel(path, sheet_name="Par commune", header=None)

    # Property block positions (fixed layout)
    PROPERTY_LAYOUT = {
        "house_all": {"start": 5},
        "house_closed": {"start": 10},
        "house_open": {"start": 15},
        "apartment": {"start": 20},
    }

    data = raw.iloc[3:].copy().reset_index(drop=True)

    frames = []
    for prop_key, cfg in PROPERTY_LAYOUT.items():
        s = cfg["start"]
        sub = pd.DataFrame({
            "refnis": data.iloc[:, 0],
            "municipality": data.iloc[:, 1],
            "year": data.iloc[:, 2],
            "quarter": data.iloc[:, 3],
            "property_type": prop_key,
            "n_transactions": data.iloc[:, s],
            "median_price": data.iloc[:, s + 1],
            "price_q1": data.iloc[:, s + 2],
            "price_q3": data.iloc[:, s + 3],
        })
        frames.append(sub)

    df = pd.concat(frames, ignore_index=True)

    # Type cleaning
    for col in ["refnis", "year", "n_transactions", "median_price", "price_q1", "price_q3"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["refnis"])
    df["refnis"] = df["refnis"].astype(int)
    df["municipality"] = df["municipality"].astype(str).str.strip()

    log.info(f"Parsed {len(df):,} rows")
    return df


def parse_refnis(path: str) -> pd.DataFrame:
    """
    Parse TU_COM_REFNIS-20250101.xlsx structure:
    LVL_REFNIS | CD_REFNIS | CD_SUP_REFNIS | TX_REFNIS_DE | TX_REFNIS_FR | TX_REFNIS_NL | ...
    """
    log.info(f"Parsing {path}")

    refnis_raw = pd.read_excel(path, sheet_name="TU_COM_REFNIS")

    refnis_raw["lvl_type"] = refnis_raw["LVL_REFNIS"].map({
        1: "region",
        2: "province",
        3: "district",
        4: "municipality"
    })

    refnis_raw["CD_SUP_REFNIS"] = refnis_raw["CD_SUP_REFNIS"].replace('-', pd.NA)

    # Filter to municipality level (LVL_REFNIS = 3) and clean
    refnis = pd.DataFrame({
        "refnis": refnis_raw["CD_REFNIS"].astype(int),
        "name_fr": refnis_raw["TX_REFNIS_FR"].astype(str).str.strip(),
        "name_nl": refnis_raw["TX_REFNIS_NL"].astype(str).str.strip(),
        "name_de": refnis_raw["TX_REFNIS_DE"].astype(str).str.strip(),
        "sup_refnis": refnis_raw["CD_SUP_REFNIS"].astype('Int64'),
        "lvl_refnis": refnis_raw["LVL_REFNIS"].astype(int),
        "lvl_type": refnis_raw["lvl_type"],
    })

    # Drop invalid codes
    refnis = refnis.dropna(subset=["refnis"])

    log.info(f"Refnis: {len(refnis):,} municipalities")
    return refnis

def write_raw_tables(df_transactions: pd.DataFrame, df_refnis: pd.DataFrame, engine: sa.Engine) -> None:
    """Write raw DataFrames to tables."""
    log.info(f"Writing {len(df_transactions):,} transactions to {STG_TRANSACTIONS_TABLE}")
    df_transactions.to_sql(STG_TRANSACTIONS_TABLE, con=engine, if_exists="replace", index=False)

    log.info(f"Writing {len(df_refnis):,} refnis rows to {STG_REFNIS_TABLE}")
    df_refnis.to_sql(STG_REFNIS_TABLE, con=engine, if_exists="replace", index=False)

    log.info("Raw tables written ✓")


def run_pipeline(df_transactions: pd.DataFrame, df_refnis: pd.DataFrame, engine: sa.Engine) -> None:
    """Enrich raw tables → intermediate table with district info."""
    log.info("Running pipeline...")

    with engine.begin() as conn:
        # Drop if exists
        conn.execute(text(f"DROP TABLE IF EXISTS {INT_TRANSACTION_TABLE}"))
        # Create enriched table
        conn.execute(text(f"""
        CREATE TABLE {INT_TRANSACTION_TABLE} AS
        SELECT 
            t.refnis as municipality_code,
            t.municipality as municipality_name_local,
            t.year,
            t.quarter,
            t.property_type,
            t.n_transactions,
            t.median_price,
            t.price_q1,
            t.price_q3,
            rm.name_fr as municipality_name_fr,
            rm.name_nl as municipality_name_nl,
            rm.name_de as municipality_name_de,
            rm.sup_refnis as district_code,
            rd.name_fr as district_name_fr,
            rd.name_nl as district_name_nl,
            rd.name_de as district_name_de,
            rd.sup_refnis as province_code,
            rp.name_fr as province_name_fr,
            rp.name_nl as province_name_nl,
            rp.name_de as province_name_de,
            rp.sup_refnis as region_code,
            rr.name_fr as region_name_fr,
            rr.name_nl as region_name_nl,
            rr.name_de as region_name_de
        FROM {STG_TRANSACTIONS_TABLE} t
        LEFT JOIN {STG_REFNIS_TABLE} rm ON t.refnis = rm.refnis AND rm.lvl_refnis = 4
        LEFT JOIN {STG_REFNIS_TABLE} rd ON rm.sup_refnis = rd.refnis AND rd.lvl_refnis = 3
        LEFT JOIN {STG_REFNIS_TABLE} rp ON rd.sup_refnis = rp.refnis AND rp.lvl_refnis = 2
        LEFT JOIN {STG_REFNIS_TABLE} rr ON rp.sup_refnis = rr.refnis AND rr.lvl_refnis = 1
        """))
    log.info("Intermediate table created")


def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline")
    parser.add_argument("--db-url", default="postgresql+psycopg2://postgres:postgres@postgres:5432/real_estate")
    parser.add_argument("--data-dir", default="./data")
    args = parser.parse_args()

    # Download
    re_path = os.path.join(args.data_dir, "FR_immo_statbel_trimestre_par_commune.xlsx")
    refnis_path = os.path.join(args.data_dir, "TU_COM_REFNIS-20250101.xlsx")
    download_file(REAL_ESTATE_URL, re_path)
    download_file(REFNIS_URL, refnis_path)

    # Parse
    df_transactions = parse_real_estate(re_path)
    df_refnis = parse_refnis(refnis_path)

    # DB
    engine = sa.create_engine(args.db_url)
    log.info(f"Using DB: {args.db_url.split('@')[-1]}")

    # Raw → Silver
    write_raw_tables(df_transactions, df_refnis, engine)
    run_pipeline(df_transactions, df_refnis, engine)

    log.info("ETL COMPLETE")
    log.info(f"Tables: {STG_TRANSACTIONS_TABLE}, {STG_REFNIS_TABLE}, {INT_TRANSACTION_TABLE}")


if __name__ == "__main__":
    main()