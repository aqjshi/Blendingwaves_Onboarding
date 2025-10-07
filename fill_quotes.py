import pytz
from urllib.parse import urlencode
import numpy as np
import time
import queue
from sqlalchemy.dialects.postgresql import insert as pg_insert
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import threading
import pandas as pd
from sqlalchemy import create_engine, insert,  text
from db import ALPHA_KEY, AV_BASE_URL, SQL_USER, SQL_PWD, SQL_HOST, SQL_PORT, SQL_DB_NAME, SessionLocal, engine

from av_client import av_client
from models import Company, Quote, Base

import io
from requests.exceptions import HTTPError

import cProfile
import pstats
from pytz import timezone, UTC

class TokenBucketRateLimiter:
    def __init__(self, rate_limit, capacity):
        self.rate = 1.0 / rate_limit
        self.capacity = capacity
        # Initialize semaphore with 0, so it blocks until tokens are added
        self.tokens = threading.Semaphore(0)
        self._add_token_loop()


    def _add_token_loop(self):
        """Adds a token to the bucket, scheduling the next addition."""
        self.tokens.release()
        
        # --- FIX: Create the timer as a daemon thread ---
        timer = threading.Timer(self.rate, self._add_token_loop)
        timer.daemon = True # This is the crucial line
        timer.start()

    def wait(self):
        """Acquires a token, blocking if necessary until one is available."""
        self.tokens.acquire()



def db_worker(result_queue: queue.Queue, engine, total_tasks: int):
    print("Database worker started.")

    list_of_dfs = []
    list_of_unique_times_dicts = [] # New list to accumulate unique_times dictionaries
    tasks_processed = 0

    while tasks_processed < total_tasks:
        try:
            # fetch_for now returns (comp_id, df_resampled, unique_times_dict)
            cid, df, unique_times_dict = result_queue.get(timeout=10)

            if not df.empty:
                df['company_id'] = cid
                list_of_dfs.append(df)
                list_of_unique_times_dicts.append(unique_times_dict) # Accumulate unique times

            tasks_processed += 1
            result_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error in DB worker: {e}")
            tasks_processed += 1
            result_queue.task_done()

    if list_of_dfs:
        print("DB worker consolidating and upserting final batch...")

        # Combine all DataFrames into one large one
        final_df = pd.concat(list_of_dfs)
        final_df['unix_ts'] = final_df.index.tz_convert('UTC').astype(np.int64) // 10**9


        # Prepare data for upsert_quotes_via_copy
        # The 'unix_ts' column is now directly available for the dictionary creation
        quote_data_dict = {}
        for cid, group_df in final_df.groupby('company_id'):
            # Convert the group_df to a dictionary of records, with unix_ts as key
            quote_data_dict[cid] = group_df.set_index('unix_ts').drop(columns=['company_id']).to_dict('index')


        # Combine all unique_times dictionaries
        combined_unique_times = {}
        for d in list_of_unique_times_dicts:
            combined_unique_times.update(d)
        upsert_quotes_via_copy_batched(quote_data_dict, engine, batch_size=50000)

    print("✅ Database worker finished.")

# 1751356800000
# 1752158976860000
def fetch_for(av, eastern, comp_id, ticker, month):
    """
    Fetches intraday data for a ticker, resamples it to 1-minute time buckets,
    forward-fills any gaps, and returns the company ID, a timezone-aware DataFrame,
    and a dict of unique time entries.
    """
    series = av.fetch_intraday(
        ticker,
        month=month,
        interval="1min",
        outputsize="full",
        extended_hours="true"
    )

    if not series:
        return comp_id, pd.DataFrame() # Return empty DataFrame if no data

    df = pd.DataFrame.from_dict(series, orient='index')
    df = df.rename(columns={
        '1. open': 'open', '2. high': 'high',
        '3. low': 'low', '4. close': 'close', '5. volume': 'volume'
    })

    df.index = pd.to_datetime(df.index)

    try:
        df.index = df.index.tz_localize(
            eastern, # Pass the pytz timezone object directly
            nonexistent='shift_forward',
            ambiguous='infer' # Or 'fold' if 'infer' continues to fail for some data
        )
    except Exception as e:
        print(f"Warning: Could not localize index for {ticker}, month {month}: {e}")
        df.index = pd.to_datetime(df.index) # Revert to naive if localization fails
        return comp_id, pd.DataFrame()


    df = df.astype(float) # Ensure columns are numeric
    df = df.sort_index(ascending=True)

    df_resampled = df.resample('1min').ffill().dropna()

    unique_times = {}
    if not df_resampled.empty:
        utc_datetimes = df_resampled.index.tz_convert('UTC').to_pydatetime()
        est_datetimes = df_resampled.index.to_pydatetime() 
        unix_timestamps = df_resampled.index.tz_convert('UTC').astype(np.int64)

        for i, uts in enumerate(unix_timestamps):
            unique_times[uts] = (utc_datetimes[i].replace(tzinfo=None), est_datetimes[i].replace(tzinfo=None))

    # Return the resampled DataFrame and the unique_times
    return comp_id, df_resampled, unique_times



def upsert_quotes_via_copy(raw_data: dict, engine):


    """
    Bulk‑upsert Quote records via COPY into a temp staging table,
    then INSERT ... ON CONFLICT DO NOTHING into the real quote table.
    raw_data: { company_id: { uts: {open,high,low,close,volume}, … }, … }
    """
    # 1) Flatten into a DataFrame
    rows = []
    for cid, by_ts in raw_data.items():
        for uts, m in by_ts.items():
            rows.append({
                "company_id":    cid,
                "time_entry_ts": uts,
                "open_price":    m["open"],
                "high_price":    m["high"],
                "low_price":     m["low"],
                "close_price":   m["close"],
                "volume":        m["volume"],
            })
    df = pd.DataFrame(rows, columns=[
        "company_id","time_entry_ts",
        "open_price","high_price","low_price","close_price","volume"
    ]).astype({
        "company_id":"int64",
        "time_entry_ts":"int64",
        "open_price":"float64",
        "high_price":"float64",
        "low_price":"float64",
        "close_price":"float64",
        "volume":"int64"
    })

    # 2) Open raw connection & cursor
    raw_conn = engine.raw_connection()
    cur      = raw_conn.cursor()

    # 3) Create temp staging table
    cur.execute("""
        CREATE TEMP TABLE quote_stage (
          company_id    bigint,
          time_entry_ts bigint,
          open_price    double precision,
          high_price    double precision,
          low_price     double precision,
          close_price   double precision,
          volume        bigint
        ) ON COMMIT DROP;
    """)

    # 4) COPY DataFrame into staging (tab‑delimited, \N for NULL)
    buf = io.StringIO()
    df.to_csv(buf, sep="\t", index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cur.copy_from(
        buf,
        "quote_stage",
        sep="\t",
        columns=[
            "company_id","time_entry_ts",
            "open_price","high_price","low_price","close_price","volume"
        ]
    )

    # 5) Single upsert from staging → real table
    cur.execute("""
        INSERT INTO quotes (
          company_id, time_entry_ts,
          open_price, high_price, low_price, close_price, volume
        )
        SELECT
          company_id, time_entry_ts,
          open_price, high_price, low_price, close_price, volume
        FROM quote_stage
        ON CONFLICT (company_id, time_entry_ts) DO NOTHING;
    """)

    # 6) Commit & clean up
    raw_conn.commit()
    cur.close()
    raw_conn.close()


def upsert_quotes_via_copy_batched(raw_data: dict, engine, batch_size: int = 100):
    """
    Bulk-upsert Quote records via COPY into a temp staging table, in batches,
    then INSERT ... ON CONFLICT DO NOTHING into the real quote table.

    raw_data: { company_id: { uts: {open,high,low,close,volume}, … }, … }
    engine: SQLAlchemy engine object for database connection
    batch_size: Number of rows to process per batch for COPY operation.
    """
    num_worker = 10
    chunk_size = 10

    if not raw_data:
        print("No quote data to upsert.")
        return

    # 1) Flatten the raw_data dictionary into a list of rows first
    # This makes it easier to iterate and batch
    all_rows = []
    for cid, by_ts in raw_data.items():
        for uts, m in by_ts.items():
            all_rows.append({
                "company_id":    cid,
                "time_entry_ts": uts,
                "open_price":    float(m["open"]),
                "high_price":    float(m["high"]),
                "low_price":     float(m["low"]),
                "close_price":   float(m["close"]),
                "volume":        int(float(m["volume"])),
            })

    if not all_rows:
        print("No flattened rows to upsert.")
        return

    # Define the columns and their dtypes for the DataFrame and COPY operation
    columns = [
        "company_id", "time_entry_ts",
        "open_price", "high_price", "low_price", "close_price", "volume"
    ]
    dtypes = {
        "company_id": "int64",
        "time_entry_ts": "int64",
        "open_price": "float64",
        "high_price": "float64",
        "low_price": "float64",
        "close_price": "float64",
        "volume": "int64"
    }

    # 2) Open raw connection & cursor once for all batches
    raw_conn = engine.raw_connection()
    cur      = raw_conn.cursor()

    try:
        # 3) Create a single transient staging table for all batches
        # ON COMMIT DROP ensures it's cleaned up after the transaction.
        cur.execute("""
            CREATE TEMP TABLE quote_stage (
              company_id      bigint,
              time_entry_ts   bigint,
              open_price      double precision,
              high_price      double precision,
              low_price       double precision,
              close_price     double precision,
              volume          bigint
            ) ON COMMIT DROP;
        """)

        # 4) Iterate through all_rows in batches and COPY into staging
        total_rows = len(all_rows)
        for i in range(0, total_rows, batch_size):
            batch_rows = all_rows[i : i + batch_size]
            print(f"Processing batch {i // batch_size + 1}/{(total_rows + batch_size - 1) // batch_size} "
                  f"({len(batch_rows)} rows)")

            # Create DataFrame for the current batch
            df_batch = pd.DataFrame(batch_rows, columns=columns).astype(dtypes)

            # COPY DataFrame into staging (tab-delimited, \N for NULL)
            buf = io.StringIO()
            df_batch.to_csv(buf, sep="\t", index=False, header=False, na_rep="\\N")
            buf.seek(0)
            cur.copy_from(
                buf,
                "quote_stage",
                sep="\t",
                columns=columns
            )
            buf.close() # Close the StringIO buffer after use

        # 5) Single upsert from staging → real table after all batches are copied
        cur.execute("""
            INSERT INTO quotes (
              company_id, time_entry_ts,
              open_price, high_price, low_price, close_price, volume
            )
            SELECT
              company_id, time_entry_ts,
              open_price, high_price, low_price, close_price, volume
            FROM quote_stage
            ON CONFLICT (company_id, time_entry_ts) DO NOTHING;
        """)

        # 6) Commit & clean up
        raw_conn.commit()
        print(f"Successfully upserted {total_rows} quotes.")

    except Exception as e:
        raw_conn.rollback() # Rollback on error
        print(f"Error during batched upsert: {e}")
        raise # Re-raise the exception after rollback

    finally:
        cur.close()
        raw_conn.close()




def fetch_worker(rate_limiter, comp_id, ticker, month):
    """
    Worker function that waits for the rate limiter before fetching data.
    """
    rate_limiter.wait()  # This will block until a "token" is available
    
    # These can be created once and passed in, but for simplicity in a thread:
    av = av_client()
    eastern = pytz.timezone("US/Eastern")
    
    print(f"Fetching {ticker} for {month}...")
    try:
        return fetch_for(av, eastern, comp_id, ticker, month)
    except HTTPError as e:
        print(f"HTTPError for {ticker}/{month}. Retrying after 5s...")
        time.sleep(5)
        return fetch_for(av, eastern, comp_id, ticker, month)



def sub_batch_function(companies, months, rate_limiter, curr_item=0, sub_batch_size=10):
    while curr_item < len(companies):
        batch = companies[curr_item : curr_item + sub_batch_size]
        print(f"\n--- Processing Batch: {curr_item} to {curr_item + len(batch) - 1} ---")
        batch_start_time = time.perf_counter()

        unique_times = {}
        raw_data = {c.id: {} for c in batch}

        # --- 1. TIME THE API FETCHING STAGE ---
        api_start_time = time.perf_counter()
        with ThreadPoolExecutor(max_workers=30) as exe: # Increase workers
            futures = [
                # Pass the rate_limiter to each worker
                exe.submit(fetch_worker, rate_limiter, c.id, c.ticker, m)
                for c in batch for m in months
            ]
            for fut in as_completed(futures):
                try:
                    cid, data, times = fut.result()
                    raw_data[cid].update(data)
                    unique_times.update(times)
                except Exception as e:
                    print(f"A fetch operation failed even after retries: {e}")
        api_elapsed_time = time.perf_counter() - api_start_time

        # --- 2. TIME THE DATABASE WRITES ---
        db_start_time = time.perf_counter()


        total_quotes = sum(len(v) for v in raw_data.values())
        if total_quotes > 0:
            upsert_quotes_via_copy(raw_data, engine)
        db_elapsed_time = time.perf_counter() - db_start_time

        batch_elapsed_time = time.perf_counter() - batch_start_time
        print(f"✅ Batch complete. Loaded {total_quotes} quotes.")
        print("--- Batch Performance ---")
        print(f"  API Fetching & Processing: {api_elapsed_time:.2f} seconds")
        print(f"  Database Upserts:          {db_elapsed_time:.2f} seconds")
        print(f"  Total Batch Time:          {batch_elapsed_time:.2f} seconds")
        
        curr_item += sub_batch_size



def fetch_and_process_worker(rate_limiter, result_queue, comp_id, ticker, month):
    """
    Worker function executed by the thread pool. Rate-limits, fetches,
    processes data, and puts the result into the queue for the DB worker.
    """
    rate_limiter.wait() 
    av = av_client()
    eastern = pytz.timezone("US/Eastern")

    print(f"Fetching {ticker} for {month}...")
    try:
        processed_result = fetch_for(av, eastern, comp_id, ticker, month)
        result_queue.put(processed_result)
    except HTTPError as e:
        print(f"HTTPError for {ticker}/{month}. Retrying after 5s...")
        time.sleep(5)
        try:
            processed_result = fetch_for(av, eastern, comp_id, ticker, month)
            result_queue.put(processed_result) 
        except Exception as retry_e:
            print(f"Retry failed for {ticker}/{month}: {retry_e}")
            result_queue.put((comp_id, {}, {})) 
    except Exception as e:
        print(f"An error occurred in worker for {ticker}/{month}: {e}")
        result_queue.put((comp_id, {}, {})) 


        
def main():
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS quotes"))
        conn.commit()
    Base.metadata.create_all(bind=engine)

    # START INTENDED EDIT
    search =  [
        "TSLA",
        "RIOT", 
        "PLTR", 
        "MRNA",
        "RGTI" ,
        "IONQ",
        "AAPL", "AMD", "MSFT", "AMZN", "GOOGL", "NVDA", "TSLA", "META", "JNJ", "JPM",
        "XOM", "WMT", "PG", "KO", "V", "UNH", "HD", "CRM", "NFLX", "SBUX",
        "BA", "GE", "F", "AAL", "GME", "BYND", "PLTR", "RIOT", "MRNA", "RBLX",
        "U"
        ]

    months = [
        "2024-01", 
              "2024-02",
              "2024-03",
              "2024-04",
              "2024-05",
              "2024-06",
              "2024-07",
              "2024-08",
              "2024-09",
              "2024-10",
              "2024-11",
              "2024-12",
              "2025-01",
              "2025-02",
              "2025-03",
                "2025-04",
                "2025-05",
              "2025-06",
              "2025-07",
              "2025-08"
              ]
    # END INTENDED EDIT
    with SessionLocal() as session:
        companies = session.query(Company).filter(Company.ticker.in_(search)).all()
    
    api_rate_limit = 5
    api_capacity = 90 # A capacity > rate allows for handling bursts
    rate_limiter = TokenBucketRateLimiter(api_rate_limit, api_capacity)


    result_queue = queue.Queue()
    tasks = [(c.id, c.ticker, m) for c in companies for m in months]
    total_tasks = len(tasks)
    print(f"Total API calls to make: {total_tasks}")

    db_thread = threading.Thread(target=db_worker, args=(result_queue, engine, total_tasks), daemon=True)
    db_thread.start()

    num_fetcher_threads = 100
    with ThreadPoolExecutor(max_workers=num_fetcher_threads) as executor:
        for comp_id, ticker, month in tasks:
            executor.submit(fetch_and_process_worker, rate_limiter, result_queue, comp_id, ticker, month)

    print("All fetcher tasks have been completed.")

    result_queue.join()

    db_thread.join()

    print("All operations complete.")



def run_main_with_profiling():
    profiler = cProfile.Profile()

    profiler.enable()
    

    start = time.perf_counter()
    main()
    elapsed = time.perf_counter() - start
    print(f"Total Elapsed Time: {elapsed:.2f} seconds")

    profiler.disable()
    
    s = io.StringIO()

    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    ps.print_stats()
    
    print("\n\n--- cProfile Performance Analysis ---")
    print(s.getvalue())
    profiler.dump_stats('program.prof')
    print("\nFull profiling stats saved to 'program.prof'")




if __name__ == "__main__":
    start = time.perf_counter()
    main()
    elapsed = time.perf_counter() - start
    print(f"Elapsed: {elapsed:.2f} seconds")