import yfinance as yf
import os
import traceback
import pandas as pd
from db.connection import get_db_connection, close_db_connection
from services.cleanup_service import delete_invalid_timeframe_rows, delete_files_in_folder
from config.paths import YAHOO_EQUITY_DIR
from config.logger import log
from services.yahoo_service import download_equity_yahoo_data_all_timeframes
from config.paths import FREQUENCIES
#################################################################################################
# Imports historical Yahoo equity price CSVs for all symbols and timeframes into the database 
# with normalization and upsert logic.
#################################################################################################
def import_equity_csv_to_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        insert_sql = """
            INSERT INTO equity_price_data
            (symbol_id, timeframe, date, open, high, low, close, adj_close, volume, is_final)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(symbol_id, timeframe, date) DO UPDATE SET
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                adj_close=excluded.adj_close,
                volume=excluded.volume
        """
        for timeframe in FREQUENCIES:

            timeframe_path = os.path.join(YAHOO_EQUITY_DIR, timeframe)
            if not os.path.exists(timeframe_path):
                log(f"No folder for timeframe '{timeframe}', skipping")
                continue

            log(f"===== IMPORTING CSV DATA FOR TIMEFRAME '{timeframe}' =====")

            rows_inserted = 0

            for csv_file in os.listdir(timeframe_path):
                if not csv_file.lower().endswith(".csv"):
                    continue

                csv_path = os.path.join(timeframe_path, csv_file)
                symbol_name = os.path.splitext(csv_file)[0]

                # Get symbol_id
                cur.execute(
                    "SELECT symbol_id FROM equity_symbols WHERE symbol = ?",
                    (symbol_name,)
                )
                res = cur.fetchone()
                if not res:
                    log(f"Symbol '{symbol_name}' not found, skipping")
                    continue

                symbol_id = res[0]

                try:
                    df = pd.read_csv(csv_path)
                    if df.empty:
                        continue

                    # Normalize column names
                    df.columns = [c.strip() for c in df.columns]

                    # Normalize date
                    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

                    # Round numeric columns
                    for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
                        if col in df.columns:
                            df[col] = df[col].round(2)

                    rows = [
                        (
                            symbol_id,
                            timeframe,
                            row["Date"],
                            row["Open"],
                            row["High"],
                            row["Low"],
                            row["Close"],
                            row["Adj Close"],
                            row["Volume"]
                        )
                        for _, row in df.iterrows()
                    ]

                    cur.executemany(insert_sql, rows)
                    rows_inserted += len(rows)

                except Exception as e:
                    log(f"❌ FAILED {symbol_name} | {timeframe} | {e}")
                    traceback.print_exc()

            # ✅ Commit once per timeframe
            conn.commit()
            log(f"✅ {timeframe}: committed {rows_inserted} rows")

        log("🎉 ALL EQUITY CSV FILES IMPORTED INTO DATABASE")

    except Exception as e:
        log(f"CRITICAL FAILURE import_equity_csv_to_db | {e}")
        traceback.print_exc()

    finally:
        close_db_connection(conn)
#################################################################################################
# Performs a full equity price refresh by downloading Yahoo data, importing it into 
# the database, cleaning invalid weekly/monthly rows, and removing temporary CSV files.
#################################################################################################
def insert_equity_price_data(symbol):
    try:
        log(f"===== YAHOO DOWNLOAD STARTED =====")
        print(f"===== YAHOO DOWNLOAD STARTED =====")
        download_equity_yahoo_data_all_timeframes(symbol)
        print(f"===== YAHOO DOWNLOAD FINISHED =====")
        log(f"===== YAHOO DOWNLOAD FINISHED =====")
        
        log(f"===== CSV TO DATABASE IMPORT STARTED =====")
        print(f"===== CSV TO DATABASE IMPORT STARTED =====")
        import_equity_csv_to_db()
        print(f"===== CSV TO DATABASE IMPORT FINISHED =====")
        log(f"===== CSV TO DATABASE IMPORT FINISHED =====")
        
        log(f"===== DELETE INVALID ROWS FOR WEEK & MONTH STARTED =====")
        print(f"===== DELETE INVALID ROWS FOR WEEK & MONTH STARTED =====")
        delete_invalid_timeframe_rows("1wk")
        delete_invalid_timeframe_rows("1mo")
        print(f"===== DELETE INVALID ROWS FOR WEEK & MONTH FINISHED =====")
        log(f"===== DELETE INVALID ROWS FOR WEEK & MONTH FINISHED =====")
        
        log(f"===== DELETE FILES FROM FOLDERS STARTED =====")
        print(f"===== DELETE FILES FROM FOLDERS STARTED =====")
        for timeframe in FREQUENCIES:
            folder_path = os.path.join(YAHOO_EQUITY_DIR, timeframe)
            delete_files_in_folder(folder_path)
        print(f"===== DELETE FILES FROM FOLDERS FINISHED =====")
        log(f"===== DELETE FILES FROM FOLDERS FINISHED =====")
        
    except Exception as e:
        log(f"ERROR: {e}")
        traceback.print_exc()

