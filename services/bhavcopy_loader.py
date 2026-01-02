import os
import requests
import traceback
import shutil
import pandas as pd
from datetime import datetime, timedelta,date
from db.connection import get_db_connection, close_db_connection
from services.cleanup_service import delete_files_in_folder
from services.symbol_service import (
    retrieve_equity_symbol, get_latest_equity_date,
    get_latest_equity_date_no_delv
)
from config.logger import log
from config.paths import BHAVCOPY_DIR,NSE_URL_BHAV_DAILY,BHAVCOPY_DIR_HIST
#################################################################################################
# Downloads the NSE bhavcopy CSV for a given date (default: today), saves it locally, 
# and returns the file path.Handles missing files (holidays/weekends) and errors 
# gracefully while logging all events.
#################################################################################################
def download_bhavcopy(date_str=None):
    """Download NSE bhavcopy for given date (ddmmyyyy)."""
    try:
        # ---- Today's bhavcopy if no date passed ----
        if date_str is None:
            date_str = datetime.now().strftime("%d%m%Y")
        # ---- Ensure folder exists ----
        os.makedirs(BHAVCOPY_DIR, exist_ok=True)
        # ---- Prepare URL and save path ----
        save_path = os.path.join(
            BHAVCOPY_DIR,
            f"sec_bhavdata_full_{date_str}.csv"
        )
        url = NSE_URL_BHAV_DAILY.format(date_str)
        log(f"⬇ Downloading bhavcopy: {date_str} -> {url}")
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv,*/*;q=0.8"
        }
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code != 200:
            log(f"❗ HTTP {response.status_code} | File missing (holiday / weekend / not available)")
            return None
        with open(save_path, "wb") as f:
            f.write(response.content)
        log(f"✔ Saved: {save_path}")
        return save_path
    except Exception as e:
        log(f"❗ Unexpected error: {e}")
        traceback.print_exc()
        return None
#################################################################################################
# Detects missing NSE bhavcopy dates from the database and downloads all required daily CSVs 
# into the bhavcopy folder.Supports override date, clears old files once, loops through dates, 
# and logs the full download summary.
#################################################################################################
def download_missing_bhavcopies(override_date=None):
    log("🚀 Starting bhavcopy download process...")

    if override_date:
        if isinstance(override_date, str):
            latest_date = datetime.strptime(override_date, "%Y-%m-%d").date()
        elif isinstance(override_date, datetime):
            latest_date = override_date.date()
        elif isinstance(override_date, date):
            latest_date = override_date
        else:
            raise ValueError(f"Unsupported override_date type: {type(override_date)}")

        log(f"⚠ OVERRIDE latest date: {latest_date}")
    else:
        latest_date = get_latest_equity_date()
    # if DB empty or override empty case
    if latest_date is None:
        log("⚠ No price data found in DB. Starting fresh from today-30days.")
        latest_date = datetime.now().date() - timedelta(days=30)

    start_date = latest_date + timedelta(days=1)
    today = datetime.now().date()

    if start_date > today:
        log("✔ No missing dates. Database already up to date.")
        return

    # ---- 🔥 CLEAR ONLY ONCE ----
    os.makedirs(BHAVCOPY_DIR, exist_ok=True)

    for filename in os.listdir(BHAVCOPY_DIR):
        file_path = os.path.join(BHAVCOPY_DIR, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            log(f"❗ Failed to delete {file_path}: {e}")

    log(f"🧹 Cleared old files in {BHAVCOPY_DIR}")

    # ---- 🔽 DOWNLOAD MULTIPLE FILES ----
    download_count = 0
    curr = start_date
    while curr <= today:
        formatted = curr.strftime("%d%m%Y")
        log(f"📌 Processing {curr.strftime('%Y-%m-%d')}")

        # download without clearing
        download_bhavcopy(formatted)

        download_count += 1
        curr += timedelta(days=1)

    log(f"🎉 Download completed. Total downloaded: {download_count}")
    print(f"🎉 Download completed. Total downloaded: {download_count}")
#################################################################################################
# Inserts/updates daily OHLCV + delivery % for all symbols in DB using downloaded bhavcopy CSVs.
# Loops CSV-by-CSV and symbol-by-symbol, maps fields, performs UPSERT into equity_price_data, 
# and logs results.
#################################################################################################
def update_equity_price_from_bhavcopy(symbol="ALL"):

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        log("🚀 Starting equity_price_data update from bhavcopy CSV files")

        # ---- Load symbols ----
        df_symbols = retrieve_equity_symbol(symbol, conn)
        if df_symbols.empty:
            log("❗ No symbols found to process")
            return

        log(f"🔎 Symbols to process: {len(df_symbols)}")

        # ---- Locate CSV files ----
        csv_files = sorted([
            f for f in os.listdir(BHAVCOPY_DIR)
            if f.endswith(".csv") and "sec_bhavdata_full_" in f
        ])

        if not csv_files:
            log("❗ No bhavcopy CSV files found to process")
            return

        insert_sql = """
            INSERT INTO equity_price_data
            (symbol_id, timeframe, date, open, high, low, close, adj_close, volume, delv_pct, is_final)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(symbol_id, timeframe, date)
            DO UPDATE SET
                open      = excluded.open,
                high      = excluded.high,
                low       = excluded.low,
                close     = excluded.close,
                adj_close = excluded.adj_close,
                volume    = excluded.volume,
                delv_pct  = excluded.delv_pct
        """

        total_updates = 0

        # ---- Process each CSV ----
        for file in csv_files:
            csv_path = os.path.join(BHAVCOPY_DIR, file)

            # ---- Extract date (ddmmyyyy) from filename ----
            try:
                date_str = file.split("_")[-1].split(".")[0]  # 31122025
                file_date = datetime.strptime(date_str, "%d%m%Y").strftime("%Y-%m-%d")
            except Exception:
                log(f"⚠ Skipping invalid filename format: {file}")
                continue

            log(f"\n📂 Processing: {file} | Date: {file_date}")

            # ---- Load CSV ----
            try:
                df_csv = pd.read_csv(csv_path)
            except Exception:
                log(f"❗ Failed reading CSV: {file}")
                continue

            if df_csv.empty:
                log(f"⚠ Empty CSV, skipping: {file}")
                continue

            # ---- Normalize column names ----
            df_csv.columns = [c.strip().upper() for c in df_csv.columns]

            # ---- Process each symbol ----
            for _, row_sym in df_symbols.iterrows():
                sid = row_sym["symbol_id"]
                sym = row_sym["symbol"]

                df_row = df_csv[df_csv["SYMBOL"] == sym]

                if df_row.empty:
                    log(f"⚠ {sym}: not found in CSV for {file_date}")
                    continue

                # df_row = df_row.iloc[0]  # first match

                # # ---- Map columns ----
                # record = (
                #     sid,
                #     "1d",
                #     file_date,
                #     df_row.get("OPEN_PRICE", None),
                #     df_row.get("HIGH_PRICE", None),
                #     df_row.get("LOW_PRICE", None),
                #     df_row.get("LAST_PRICE", None),
                #     df_row.get("CLOSE_PRICE", None),
                #     df_row.get("TTL_TRD_QNTY", None),
                #     df_row.get("DELIV_PER", None),
                # )
                df_row = df_row.iloc[0]

                # --- fix numeric fields ---
                clean_int = lambda x: int(str(x).replace(",", "")) if pd.notna(x) else None
                clean_float = lambda x: float(str(x).replace(",", "")) if pd.notna(x) else None

                open_p  = clean_float(df_row.get("OPEN_PRICE"))
                high_p  = clean_float(df_row.get("HIGH_PRICE"))
                low_p   = clean_float(df_row.get("LOW_PRICE"))
                close_p = clean_float(df_row.get("LAST_PRICE"))
                adj_c   = clean_float(df_row.get("CLOSE_PRICE"))
                volume  = clean_int(df_row.get("TTL_TRD_QNTY"))
                delv    = clean_float(df_row.get("DELIV_PER"))

                record = (sid, "1d", file_date, open_p, high_p, low_p, close_p, adj_c, volume, delv)

                cur.execute(insert_sql, record)
                total_updates += 1

                log(f"✔ {sym:<12} updated for {file_date}")

        conn.commit()
        log(f"\n🎉 Update complete — total DB rows inserted/updated: {total_updates}")
        print(f"\n🎉 Update complete — total DB rows inserted/updated: {total_updates}")

    except Exception as e:
        log(f"❗ ERROR during update: {e}")
        conn.rollback()

    finally:
        close_db_connection(conn)
        log("🔚 DB connection closed")
#################################################################################################
# Reads bhavcopy CSVs and updates only the `delv_pct` field in `equity_price_data` 
# for matching dates/symbols,inserting missing rows and leaving all price fields untouched.
#################################################################################################
def update_equity_delv_pct_from_bhavcopy(symbol="ALL"):

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        log("🚀 Starting DELV_PCT update from bhavcopy CSV files")

        # ---- Load symbols ----
        df_symbols = retrieve_equity_symbol(symbol, conn)
        if df_symbols.empty:
            log("❗ No symbols found to process")
            return

        log(f"🔎 Symbols to process: {len(df_symbols)}")

        # ---- Locate CSV files ----
        csv_files = sorted([
            f for f in os.listdir(BHAVCOPY_DIR)
            if f.endswith(".csv") and "sec_bhavdata_full_" in f
        ])

        if not csv_files:
            log("❗ No bhavcopy CSV files found to process")
            return

        # ---- SQL: only update/insert delv_pct ----
        sql_delv = """
            INSERT INTO equity_price_data (symbol_id, timeframe, date, delv_pct, is_final)
            VALUES (?, '1d', ?, ?, 1)
            ON CONFLICT(symbol_id, timeframe, date)
            DO UPDATE SET
                delv_pct = excluded.delv_pct
        """

        total_updates = 0

        # ---- Process each CSV ----
        for file in csv_files:
            csv_path = os.path.join(BHAVCOPY_DIR, file)

            # extract date from filename
            try:
                date_str = file.split("_")[-1].split(".")[0]  # 31122025
                file_date = datetime.strptime(date_str, "%d%m%Y").strftime("%Y-%m-%d")
            except Exception:
                log(f"⚠ Skipping invalid filename format: {file}")
                continue

            log(f"\n📂 Processing: {file} | Date: {file_date}")

            try:
                df_csv = pd.read_csv(csv_path)
            except Exception:
                log(f"❗ Failed to read CSV: {file}")
                continue

            if df_csv.empty:
                log(f"⚠ Empty CSV, skipping")
                continue

            df_csv.columns = [c.strip().upper() for c in df_csv.columns]

            # ---- Process each symbol ----
            for _, row_sym in df_symbols.iterrows():
                sid = row_sym["symbol_id"]
                sym = row_sym["symbol"]

                df_row = df_csv[df_csv["SYMBOL"] == sym]
                if df_row.empty:
                    continue

                df_row = df_row.iloc[0]

                # clean delv_pct only
                try:
                    delv = float(str(df_row.get("DELIV_PER")).replace(",", "")) \
                            if pd.notna(df_row.get("DELIV_PER")) else None
                except:
                    delv = None

                cur.execute(sql_delv, (sid, file_date, delv))
                total_updates += 1

                log(f"✔ {sym:<12} delv_pct updated for {file_date}")

        conn.commit()
        log(f"\n🎉 DELV_PCT update complete — total rows affected: {total_updates}")
        print(f"\n🎉 DELV_PCT update complete — total rows affected: {total_updates}")

    except Exception as e:
        log(f"❗ ERROR during delv update: {e}")
        conn.rollback()

    finally:
        close_db_connection(conn)
        log("🔚 DB connection closed")
#################################################################################################
# Updates only `delv_pct` in equity_price_data using historical bhavcopy files 
# named <SYMBOL>_*.csv. Reads each CSV, parses Date → yyyy-mm-dd, matches by symbol_id + date, 
# and performs UPDATE.
#################################################################################################
def update_hist_delv_pct_from_bhavcopy():
    TIMEFRAME = "1d"
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # --- get symbol_id mapping for faster lookup
        symbols = retrieve_equity_symbol("ALL", conn)  # expected list of dicts or tuples
        symbol_map = {row["symbol"].upper(): row["symbol_id"] for _, row in symbols.iterrows()}
        print(f"Loaded {len(symbol_map)} symbols from DB")

        # --- loop bhavcopy folder
        for file_name in os.listdir(BHAVCOPY_DIR_HIST):
            if not file_name.lower().endswith(".csv"):
                continue

            # extract symbol part: CUPID_29DEC2025.csv → CUPID
            symbol = file_name.split("_")[0].upper()
            if symbol not in symbol_map:
                print(f"⚠️ Symbol in file not found in DB: {symbol}")
                continue

            symbol_id = symbol_map[symbol]
            csv_path = BHAVCOPY_DIR_HIST / file_name

            try:
                df = pd.read_csv(csv_path)
                # normalize headers (remove spaces, lowercase)
                df.columns = [col.strip() for col in df.columns]

                # required CSV columns
                if "Date" not in df.columns or "% Dly Qt to Traded Qty" not in df.columns:
                    print(f"❌ Missing required columns in {file_name}")
                    continue

                # iterate rows for this symbol CSV
                for _, row in df.iterrows():
                    try:
                        # Convert formats like '24-Aug-2025' → '2025-08-24'
                        date_obj = pd.to_datetime(row["Date"], format="%d-%b-%Y").strftime("%Y-%m-%d")
                        delv_pct = row["% Dly Qt to Traded Qty"]
                    
                        cur.execute("""
                            UPDATE equity_price_data
                            SET delv_pct = ?
                            WHERE symbol_id = ?
                            AND timeframe = ?
                            AND date = DATE(?)
                        """, (delv_pct, symbol_id, TIMEFRAME, date_obj))
                            
                    except Exception as e:
                        print(f"❌ DB update error for {symbol} {date_obj}: {e}")
                        traceback.print_exc()

                print(f"✔ Updated delv_pct for {symbol}")

            except Exception as e:
                print(f"❌ Failed reading {file_name}: {e}")

        conn.commit()
        print("🎉 Done updating delivery percentages.")
    except Exception as e:
        log(f"❗ ERROR during update: {e}")
        traceback.print_exc(0)
    finally:
        close_db_connection(conn)
#################################################################################################
# Finds the latest date where delivery % is missing, downloads only those bhavcopies, 
# updates `delv_pct` in the database, and cleans up downloaded files.
#################################################################################################
def update_latest_delv_pct_from_bhavcopy():
    try:
        latest_date = get_latest_equity_date_no_delv()

        # ✅ NULL GUARD — nothing to update
        if latest_date is None:
            log("✔ All delivery percentages already present. Nothing to update.")
            return

        latest_date_str = latest_date.strftime("%Y-%m-%d")

        log(f"🔍 Missing delv_pct detected after {latest_date_str}")
        
        log(f"===== DOWNLOAD MISSING BHAVCOPY STARTED =====")
        print(f"===== DOWNLOAD MISSING BHAVCOPY STARTED =====")
        download_missing_bhavcopies(latest_date_str)
        print(f"===== DOWNLOAD MISSING BHAVCOPY FINISHED =====")
        log(f"===== DOWNLOAD MISSING BHAVCOPY FINISHED =====")

        log(f"===== UPDATE EQUITY DELIVERY PERCENTAGE STARTED =====")
        print(f"===== UPDATE EQUITY DELIVERY PERCENTAGE STARTED =====")
        update_equity_delv_pct_from_bhavcopy()
        print(f"===== UPDATE EQUITY DELIVERY PERCENTAGE FINISHED =====")
        log(f"===== UPDATE EQUITY DELIVERY PERCENTAGE FINISHED =====")
        
        log(f"===== DELETING FILES STARTED =====")
        print(f"===== DELETING FILES STARTED =====")
        delete_files_in_folder(BHAVCOPY_DIR)
        print(f"===== DELETING FILES FINISHED =====")
        log(f"===== DELETING FILES FINISHED =====")
        
    except Exception as e:
        log(f"❗ ERROR: {e}")
        traceback.print_exc(0)
        