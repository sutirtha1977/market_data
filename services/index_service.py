import yfinance as yf
import os
import traceback
import pandas as pd
from db.connection import get_db_connection, close_db_connection
from services.cleanup_service import delete_invalid_timeframe_rows, delete_files_in_folder
from config.paths import YAHOO_INDEX_DIR
from config.logger import log
from services.yahoo_service import download_index_yahoo_data_all_timeframes
from config.paths import FREQUENCIES

#################################################################################################
# Imports Yahoo-downloaded index CSV files across all timeframes into the 
# index_price_data table using upsert logic with full validation and logging.
#################################################################################################  
def import_index_csv_to_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for timeframe in FREQUENCIES:
            timeframe_path = os.path.join(YAHOO_INDEX_DIR, timeframe)
            if not os.path.exists(timeframe_path):
                log(f"No folder for timeframe '{timeframe}', skipping")
                continue

            log(f"===== IMPORTING INDEX CSV DATA FOR TIMEFRAME '{timeframe}' =====")

            # Iterate over all CSV files in this timeframe folder
            for csv_file in os.listdir(timeframe_path):
                if not csv_file.lower().endswith(".csv"):
                    continue

                csv_path = os.path.join(timeframe_path, csv_file)
                index_code = os.path.splitext(csv_file)[0]

                # Lookup index_id
                cur.execute("SELECT index_id FROM index_symbols WHERE index_code = ?", (index_code,))
                res = cur.fetchone()
                if not res:
                    log(f"Index '{index_code}' not found in index_symbols table, skipping")
                    continue
                index_id = res[0]

                try:
                    df = pd.read_csv(csv_path)
                    if df.empty:
                        log(f"{index_code} | {timeframe} | CSV empty, skipping")
                        continue
                    if "Date" not in df.columns:
                        log(f"{index_code} | {timeframe} | Missing Date column, skipping")
                        continue
                    # Clean column names
                    df.columns = [c.strip() for c in df.columns]

                    # Normalize date column
                    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

                    # Round numeric columns (Volume not present for index data)
                    for col in ["Open", "High", "Low", "Close", "Adj Close"]:
                        if col in df.columns:
                            df[col] = df[col].round(2)

                    # Prepare values
                    rows = [
                        (
                            index_id,
                            timeframe,
                            row["Date"],
                            row.get("Open"),
                            row.get("High"),
                            row.get("Low"),
                            row.get("Close"),
                            row.get("Adj Close")
                        )
                        for _, row in df.iterrows()
                    ]

                    # Insert or update
                    insert_sql = """
                        INSERT INTO index_price_data
                        (index_id, timeframe, date, open, high, low, close, adj_close)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(index_id, timeframe, date) DO UPDATE SET
                            open=excluded.open,
                            high=excluded.high,
                            low=excluded.low,
                            close=excluded.close,
                            adj_close=excluded.adj_close
                    """

                    conn.executemany(insert_sql, rows)
                    conn.commit()

                    # log(f"{index_code} | {timeframe} | INSERTED/UPDATED {len(rows)} rows")

                except Exception as e:
                    log(f"❌ FAILED {index_code} | {timeframe} | {e}")
                    traceback.print_exc()

        log("🎉 ALL INDEX CSV FILES IMPORTED INTO DATABASE")

    except Exception as e:
        log(f"CRITICAL FAILURE import_index_csv_to_db | {e}")
        traceback.print_exc()

    finally:
        close_db_connection(conn)
#################################################################################################
# Downloads index price data from Yahoo, imports it into the database, 
# cleans invalid weekly/monthly rows, and removes processed CSV files.
#################################################################################################  
def insert_index_price_data():
    try:
        log(f"===== YAHOO DOWNLOAD STARTED =====")
        print(f"===== YAHOO DOWNLOAD STARTED =====")
        download_index_yahoo_data_all_timeframes()
        print(f"===== YAHOO DOWNLOAD FINISHED =====")
        log(f"===== YAHOO DOWNLOAD FINISHED =====")
        
        log(f"===== CSV TO DATABASE IMPORT STARTED =====")
        print(f"===== CSV TO DATABASE IMPORT STARTED =====")
        import_index_csv_to_db()
        print(f"===== CSV TO DATABASE IMPORT FINISHED =====")
        log(f"===== CSV TO DATABASE IMPORT FINISHED =====")
        
        log(f"===== DELETE INVALID ROWS FOR WEEK & MONTH STARTED =====")
        print(f"===== DELETE INVALID ROWS FOR WEEK & MONTH STARTED =====")
        delete_invalid_timeframe_rows("1wk", is_index=True)
        delete_invalid_timeframe_rows("1mo", is_index=True)
        print(f"===== DELETE INVALID ROWS FOR WEEK & MONTH FINISHED =====")
        log(f"===== DELETE INVALID ROWS FOR WEEK & MONTH FINISHED =====")
        
        log(f"===== DELETE FILES FROM WEEKLY AND MONTHLY FOLDERS STARTED =====")
        print(f"===== DELETE FILES FROM WEEKLY AND MONTHLY FOLDERS STARTED =====")
        for timeframe in FREQUENCIES:
            folder_path = os.path.join(YAHOO_INDEX_DIR, timeframe)
            delete_files_in_folder(folder_path)
        print(f"===== DELETE FILES FROM WEEKLY AND MONTHLY FOLDERS FINISHED =====")
        log(f"===== DELETE FILES FROM WEEKLY AND MONTHLY FOLDERS FINISHED =====")
    
    except Exception as e:
        log(f"ERROR: {e}")   
        traceback.print_exc()