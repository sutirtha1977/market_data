import os
import traceback
import shutil
import pandas as pd
import yfinance as yf
from datetime import datetime, date, timedelta, timezone
from config.logger import log
from db.connection import get_db_connection, close_db_connection
from services.symbol_service import retrieve_equity_symbol
from config.paths import FREQUENCIES, YAHOO_INDEX_DIR, YAHOO_EQUITY_DIR

SKIP_MONTHLY = (date.today().day != 1)
today_weekday = datetime.now(timezone.utc).weekday()
SKIP_WEEKLY = today_weekday != 0   # Monday = 0
# SKIP_MONTHLY = False
# SKIP_WEEKLY = False
#################################################################################################
# Downloads full historical Yahoo Finance equity data for all symbols 
# and all supported timeframes and saves them as CSV files.
#################################################################################################    
def download_equity_yahoo_data_all_timeframes(symbol):
    try:
        conn = get_db_connection()
        # --- fetch symbol list ---
        symbols_df = retrieve_equity_symbol(symbol, conn)
        if symbols_df.empty:
            log("NO SYMBOLS FOUND")
            return
        for timeframe in FREQUENCIES:
            # --- create folder for timeframe ---
            timeframe_path = os.path.join(YAHOO_EQUITY_DIR, timeframe)
            os.makedirs(timeframe_path, exist_ok=True)
            for _, row in symbols_df.iterrows():
                symbol_name = row["symbol"]
                yahoo_symbol = f"{symbol_name}.NS"
                csv_path = os.path.join(timeframe_path, f"{symbol_name}.csv")
                try:
                    # --- download full data from yahoo finance ---
                    print(f"Downloading {yahoo_symbol} | {timeframe}")
                    df = yf.download(
                        yahoo_symbol,
                        period="max",
                        interval=timeframe,
                        auto_adjust=False,
                        progress=False
                    )

                    if df is None or df.empty:
                        log(f"{yahoo_symbol} | {timeframe} | NO DATA")
                        continue
                    # --- fix: drop multi-index ticker level so only Open/High/Low/etc stay ---
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(1)
                    # --- save dataframe to csv with date index as column ---
                    df.reset_index(inplace=True)
                    df.to_csv(csv_path, index=False)

                except Exception as e:
                    log(f"❌ DOWNLOAD FAILED | {yahoo_symbol} {timeframe} | {e}")

        log("🎉 CSV EXPORT COMPLETE FOR ALL TIMEFRAMES")

    except Exception as e:
        log(f"DOWNLOAD FAILED: {e}")
        traceback.print_exc()
    finally:
        close_db_connection(conn)
#################################################################################################
# Downloads incremental Yahoo Finance equity data from the last stored date up to today 
# for all timeframes and exports CSVs.
#################################################################################################        
def download_equity_yahoo_incr_data_all_timeframes(latest_dt,symbol="ALL"):
    try:
        conn = get_db_connection()
        # # --- fetch symbol list ---
        symbols_df = retrieve_equity_symbol(symbol, conn)
        if symbols_df.empty:
            log("NO SYMBOLS FOUND")
            return
        if isinstance(latest_dt, str):
            start_date = datetime.strptime(latest_dt, "%Y-%m-%d").date()
        elif isinstance(latest_dt, date):
            start_date = latest_dt
        else:
            raise ValueError(f"Unsupported latest_dt type: {type(latest_dt)}")
        
        start_date += timedelta(days=1)
        end_date = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")

        for timeframe in FREQUENCIES:
            # MONTHLY SKIP CONTROL
            if SKIP_MONTHLY and timeframe == "1mo":
                log("Skipping 1mo (monthly-skip mode)")
                continue
            # WEEKLY SKIP CONTROL
            if SKIP_WEEKLY and timeframe == "1wk":
                log("Skipping 1wk (weekly-skip mode)")
                continue
            # --- create folder for timeframe ---
            timeframe_path = os.path.join(YAHOO_EQUITY_DIR, timeframe)
            os.makedirs(timeframe_path, exist_ok=True)
            # ---- 🔥 CLEAR ONLY ONCE ----
            for filename in os.listdir(timeframe_path):
                file_path = os.path.join(timeframe_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    log(f"❗ Failed to delete {file_path}: {e}")

            log(f"🧹 Cleared old files in {timeframe_path}")
            
            for _, row in symbols_df.iterrows():
                symbol_name = row["symbol"]
                yahoo_symbol = f"{symbol_name}.NS"
                csv_path = os.path.join(timeframe_path, f"{symbol_name}.csv")
                try:
                    # --- download full data from yahoo finance ---
                    print(f"Downloading {yahoo_symbol} | {timeframe}")
                    df = yf.download(
                        yahoo_symbol,
                        start=start_date,
                        end=end_date,
                        interval=timeframe,
                        auto_adjust=False,
                        progress=False
                    )

                    if df is None or df.empty:
                        log(f"{yahoo_symbol} | {timeframe} | NO DATA")
                        continue
                    # --- fix: drop multi-index ticker level so only Open/High/Low/etc stay ---
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(1)
                    # --- save dataframe to csv with date index as column ---
                    df.reset_index(inplace=True)
                    df.to_csv(csv_path, index=False)

                except Exception as e:
                    log(f"❌ DOWNLOAD FAILED | {yahoo_symbol} {timeframe} | {e}")

        log("🎉 CSV EXPORT COMPLETE FOR ALL TIMEFRAMES")

    except Exception as e:
        log(f"DOWNLOAD FAILED: {e}")
        traceback.print_exc()
    finally:
        close_db_connection(conn)

#################################################################################################
# Downloads full historical Yahoo Finance data for all active indices across 
# all supported timeframes and saves them as CSV files.
#################################################################################################       
def download_index_yahoo_data_all_timeframes():
    try:
        conn = get_db_connection()

        # --- fetch active indices ---
        cur = conn.cursor()
        cur.execute("""
            SELECT index_id, index_code, yahoo_symbol
            FROM index_symbols
            WHERE is_active = 1
        """)
        indices = cur.fetchall()

        if not indices:
            log("NO ACTIVE INDICES FOUND")
            return

        for timeframe in FREQUENCIES:
            log(f"===== DOWNLOADING full '{timeframe}' data for all indices =====")

            # --- create folder for timeframe ---
            timeframe_path = os.path.join(YAHOO_INDEX_DIR, timeframe)
            os.makedirs(timeframe_path, exist_ok=True)

            for index_id, index_code, yahoo_symbol in indices:
                csv_path = os.path.join(timeframe_path, f"{index_code}.csv")
                log(f"Downloading {yahoo_symbol} | {timeframe}")

                try:
                    # full download from yahoo
                    df = yf.download(
                        yahoo_symbol,
                        period="max",
                        interval=timeframe,
                        auto_adjust=False,
                        progress=False
                    )

                    if df is None or df.empty:
                        log(f"{index_code} | {timeframe} | NO DATA")
                        continue

                    # drop multi-index column level
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(1)

                    # reset index for CSV export
                    df.reset_index(inplace=True)

                    df.to_csv(csv_path, index=False)

                except Exception as e:
                    log(f"❌ DOWNLOAD FAILED | {index_code} {timeframe} | {e}")

        log("🎉 CSV EXPORT COMPLETE FOR ALL INDICES + ALL TIMEFRAMES")

    except Exception as e:
        log(f"INDEX CSV DOWNLOAD FAILED: {e}")

    finally:
        close_db_connection(conn)  
#################################################################################################
# Downloads incremental Yahoo Finance index data from the last stored date 
# up to today for all timeframes and exports CSVs.
#################################################################################################       
def download_index_yahoo_incr_data_all_timeframes(latest_dt):
    try:
        conn = get_db_connection()

        # --- fetch active indices ---
        cur = conn.cursor()
        cur.execute("""
            SELECT index_id, index_code, yahoo_symbol
            FROM index_symbols
            WHERE is_active = 1
        """)
        indices = cur.fetchall()

        if not indices:
            log("NO ACTIVE INDICES FOUND")
            return
        
        if isinstance(latest_dt, str):
            start_date = datetime.strptime(latest_dt, "%Y-%m-%d").date()
        elif isinstance(latest_dt, date):
            start_date = latest_dt
        else:
            raise ValueError(f"Unsupported latest_dt type: {type(latest_dt)}")
        # Start from next day to avoid duplicates
        start_date += timedelta(days=1)
        end_date = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        for timeframe in FREQUENCIES:
            # MONTHLY SKIP CONTROL
            if SKIP_MONTHLY and timeframe == "1mo":
                log("Skipping 1mo (monthly-skip mode)")
                continue
            # WEEKLY SKIP CONTROL
            if SKIP_WEEKLY and timeframe == "1wk":
                log("Skipping 1wk (weekly-skip mode)")
                continue
            log(f"===== DOWNLOADING full '{timeframe}' data for all indices =====")

            # --- create folder for timeframe ---
            timeframe_path = os.path.join(YAHOO_INDEX_DIR, timeframe)
            os.makedirs(timeframe_path, exist_ok=True)
            # ---- 🔥 CLEAR ONLY ONCE ----
            for filename in os.listdir(timeframe_path):
                file_path = os.path.join(timeframe_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    log(f"❗ Failed to delete {file_path}: {e}")

            log(f"🧹 Cleared old files in {timeframe_path}")
            
            for index_id, index_code, yahoo_symbol in indices:
                csv_path = os.path.join(timeframe_path, f"{index_code}.csv")
                log(f"Downloading {yahoo_symbol} | {timeframe}")

                try:
                    # full download from yahoo
                    df = yf.download(
                        yahoo_symbol,
                        start=start_date,
                        end=end_date,
                        interval=timeframe,
                        auto_adjust=False,
                        progress=False
                    )

                    if df is None or df.empty:
                        log(f"{index_code} | {timeframe} | NO DATA")
                        continue

                    # drop multi-index column level
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(1)

                    # reset index for CSV export
                    df.reset_index(inplace=True)

                    df.to_csv(csv_path, index=False)

                except Exception as e:
                    log(f"❌ DOWNLOAD FAILED | {index_code} {timeframe} | {e}")

        log("🎉 CSV EXPORT COMPLETE FOR ALL INDICES + ALL TIMEFRAMES")

    except Exception as e:
        log(f"INDEX CSV DOWNLOAD FAILED: {e}")

    finally:
        close_db_connection(conn)  