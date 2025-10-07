import os
import re
import json
from urllib.parse import urlencode
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from sqlalchemy import create_engine, insert,  text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

from db import ALPHA_KEY, AV_BASE_URL, SQL_USER, SQL_PWD, SQL_HOST, SQL_PORT, SQL_DB_NAME, SessionLocal, engine

from models import Company , Base # should have columns: ticker (pk), name, desc
from av_client import av_client




def fetch_sp500_tickers():
    url   = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    df    = pd.read_html(url, header=0)[0]
    rows  = []
    for _, r in df.iterrows():
        raw = r["Symbol"]
        # drop any suffix after a dot: e.g. BRK.B → BRK
        ticker   = re.split(r"\.", raw)[0].upper()
        # name     = r["Security"]
        # category = r["GICS Sector"]
        rows.append((ticker))
    return rows


def enrich_with_av(tuples, max_workers=5):
    client = av_client()
    results = []

    def check(ticker):
        matches = client.symbol_search(ticker)
        # find exact match on "1. symbol"
        for m in matches:
            if m.get("1. symbol", "").upper() == ticker:
                return (ticker)
        # if no exact match, take the first match if available
        if matches:
            first_match = matches[0]
            return (first_match.get("1. symbol", "").upper())
        return None

    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = {exe.submit(check, t): t for t in tuples}
        for f in as_completed(futures):
            res = f.result()
            if res:
                results.append(res)
            else:
                bad = futures[f]
                print(f"[WARN] AV lookup failed for {bad[0]}")

    return results


def upsert_companies(companies_tickers): # Renamed 'companies' to 'companies_tickers' for clarity
    """
    companies_tickers: list of tickers (strings)
    """
    rows = [
        {"ticker": t}
        for t in companies_tickers
    ]

    stmt = pg_insert(Company).values(rows)
    # Use on_conflict_do_nothing() because there's nothing to update if ticker exists
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["ticker"]
    )

    with SessionLocal() as session:
        session.execute(stmt)
        session.commit()

# ─── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    start_new =  True
    if start_new:
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)   
    
    print("⏳ Fetching S&P 500 list…")
    sp500 = fetch_sp500_tickers() + ["RGTI", "IONQ"]
    print(f"⏳ Looking up {len(sp500)} symbols via Alpha Vantage…")
    valid = enrich_with_av(sp500, max_workers=10)
    print(f"✅ {len(valid)} tickers validated by AV.")

    print("⏳ Upserting into database…")
    upsert_companies(valid)
    print("🎉 Done.")
