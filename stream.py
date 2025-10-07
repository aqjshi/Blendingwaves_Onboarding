import time
import threading
import queue
from datetime import datetime, timedelta
import pytz
import json
import numpy as np
import pandas as pd
import io
from requests.exceptions import HTTPError

# Import your SQLAlchemy components
from sqlalchemy import create_engine, text, Column, BigInteger, Integer, Float, ForeignKey, UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

from db import ALPHA_KEY, AV_BASE_URL, SQL_USER, SQL_PWD, SQL_HOST, SQL_PORT, SQL_DB_NAME, SessionLocal, engine

from av_client import av_client, process_REALTIME_BULK_QUOTES
from models import Company, Quote, Base, SubsecondQuote

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import threading

# --- Your existing upsert_quotes_via_copy_batched, renamed and adapted for SubsecondQuote ---
# This function is the one that needs to write to 'subsecond_quotes' table
def upsert_subsecond_quotes_via_copy_batched(raw_data: dict, engine, batch_size: int = 5000):
    if not raw_data:
        print("No subsecond quote data to upsert.")
        return

    all_rows = []
    for cid, by_ts in raw_data.items():
        for uts, m in by_ts.items():
            all_rows.append({
                "company_id":      cid,
                "time_entry_ts":   uts,
                "close_price":     float(m["close"]),
                "volume":          int(float(m["volume"])),
            })

    if not all_rows:
        print("No flattened rows to upsert for subsecond quotes.")
        return

    columns = [
        "company_id", "time_entry_ts",
        "close_price", "volume"
    ]
    dtypes = {
        "company_id": "int64",
        "time_entry_ts": "int64", # Ensure this matches your model (e.g., microseconds)
        "close_price": "float64",
        "volume": "int64"
    }

    raw_conn = engine.raw_connection()
    cur = raw_conn.cursor()

    try:
        cur.execute("""
            CREATE TEMP TABLE subsecond_quote_stage (
              company_id      bigint,
              time_entry_ts   bigint,
              close_price     double precision,
              volume          bigint
            ) ON COMMIT DROP;
        """)

        total_rows = len(all_rows)
        for i in range(0, total_rows, batch_size):
            batch_rows = all_rows[i : i + batch_size]
            # print(f"Processing subsecond batch {i // batch_size + 1}/{(total_rows + batch_size - 1) // batch_size} "
            #       f"({len(batch_rows)} rows)")

            df_batch = pd.DataFrame(batch_rows, columns=columns).astype(dtypes)

            buf = io.StringIO()
            df_batch.to_csv(buf, sep="\t", index=False, header=False, na_rep="\\N")
            buf.seek(0)
            cur.copy_from(
                buf,
                "subsecond_quote_stage", # Target the new staging table
                sep="\t",
                columns=columns
            )
            buf.close()

        # Insert from staging into the actual subsecond_quotes table
        cur.execute("""
            INSERT INTO subsecond_quotes (
              company_id, time_entry_ts,
              close_price, volume
            )
            SELECT
              company_id, time_entry_ts,
              close_price, volume
            FROM subsecond_quote_stage
            ON CONFLICT (company_id, time_entry_ts) DO NOTHING;
        """)

        raw_conn.commit()
        # print(f"Successfully upserted {total_rows} subsecond quotes.")

    except Exception as e:
        raw_conn.rollback()
        print(f"Error during batched subsecond upsert: {e}")
        raise

    finally:
        cur.close()
        raw_conn.close()




def main():
    # Drop and recreate subsecond_quotes table
    # with engine.connect() as conn:
    #     conn.execute(text("DROP TABLE IF EXISTS subsecond_quotes"))
    #     conn.commit()
    Base.metadata.create_all(bind=engine)

    # Populate companies table and create ticker->id map
    search_tickers = [
            "AAPL", "AMD", "MSFT", "AMZN", "GOOGL", "NVDA", "TSLA", "META", "JNJ", "JPM",
            "XOM", "WMT", "PG", "KO", "V", "UNH", "HD", "CRM", "NFLX", "SBUX",
            "BA", "GE", "F", "AAL", "GME", "BYND", "PLTR", "RIOT", "MRNA", "RBLX",
            "U", "IONQ", "RGTI"
        ]
    
    with SessionLocal() as session:
        # Query existing companies
        companies = session.query(Company).filter(Company.ticker.in_(search_tickers)).all()
        existing_tickers = {c.ticker for c in companies}
        # Find missing tickers
        missing_tickers = set(search_tickers) - existing_tickers
        # Create missing companies
        if missing_tickers:
            new_companies = [Company(ticker=t) for t in missing_tickers]
            session.add_all(new_companies)
            session.commit()
            # Re-query to get all companies including new ones
            companies = session.query(Company).filter(Company.ticker.in_(search_tickers)).all()
    company_map = {c.ticker: c.id for c in companies}

    av = av_client()
    batch_size = 50
    est = pytz.timezone('US/Eastern') 
    start_time = time.time()
    while time.time() - start_time < 23400:
        # Call bulk quotes API for all tickers at once with retry on failure
        max_retries = 5
        for attempt in range(max_retries):
            try:
                realtime_data_batch = process_REALTIME_BULK_QUOTES(av, search_tickers, extended_hours=True)
                break
            except HTTPError as e:
                print(f"Warning: API call failed with error: {e}")
                if attempt < max_retries - 1:
                    print("Waiting 5 seconds before retrying...")
                    time.sleep(5)
                else:
                    print("Max retries reached. Skipping this batch.")
                    realtime_data_batch = None

        # Prepare data for upsert
        raw_data = {}
        if realtime_data_batch:
            for ticker, quote_entry in realtime_data_batch.items():
                if ticker in company_map:
                    cid = company_map[ticker]
                    # Convert EST string timestamp to UTC then to unix microseconds
          
                    dt_est = quote_entry['timestamp']
        
                    dt_utc = dt_est.astimezone(pytz.utc)
                    time_entry_ts = int(dt_utc.timestamp() * 1_000_000)
                    # print("CID: ", cid)
                    # print("EST: ", dt_est)
                    # print("UTC: ", dt_utc)
                    # print("TIMEENTRYTS: ", time_entry_ts)
                    raw_data.setdefault(cid, {})[time_entry_ts] = {
                        "open": quote_entry['open'],
                        "high": quote_entry['high'],
                        "low": quote_entry['low'],
                        "close": quote_entry['close'],
                        "volume": quote_entry['volume']
                    }

        if raw_data:
            upsert_subsecond_quotes_via_copy_batched(raw_data, engine, batch_size=batch_size)




if __name__ == "__main__":
    start = time.perf_counter()
    main()
    elapsed = time.perf_counter() - start