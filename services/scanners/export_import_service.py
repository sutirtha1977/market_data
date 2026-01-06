import os
import pandas as pd
import traceback
from datetime import datetime
from db.connection import get_db_connection, close_db_connection
from config.logger import log
from config.paths import SCANNER_FOLDER, DB_EXPORTS

TABLES_TO_EXPORT = [
    "equity_symbols",
    "equity_price_data",
    "equity_indicators",
    "index_symbols",
    "index_price_data",
    "index_indicators"   
]

#################################################################################################
# Saves a pandas DataFrame as a timestamped CSV in the scanner folder, 
# ensuring the folder exists and logging success or errors
#################################################################################################  
def export_to_csv(df: pd.DataFrame, folder: str, base_name: str) -> str:
    try:
        # Ensure folder exists
        os.makedirs(folder, exist_ok=True)

        # Generate filename with timestamp
        ts = datetime.now().strftime("%d%b%Y")
        filename = f"{base_name}_{ts}.csv"
        filepath = os.path.join(folder, filename)

        # Save CSV
        df.to_csv(filepath, index=False)
        log(f"✔ CSV saved at {filepath}")

        return os.path.abspath(filepath)

    except Exception as e:
        log(f"❌ CSV export failed | {e}")
        traceback.print_exc()
        return ""
    
#################################################################################################
# Exports selected SQLite tables to CSV files (one file per table).
#################################################################################################  
def export_selected_tables():
    # -------------------------------------------------
    # Ensure export folder exists
    # -------------------------------------------------
    os.makedirs(DB_EXPORTS, exist_ok=True)

    # -------------------------------------------------
    # 🔥 Clean existing exported files
    # -------------------------------------------------
    for file in os.listdir(DB_EXPORTS):
        file_path = os.path.join(DB_EXPORTS, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
            log(f"🗑 Deleted existing file: {file}")

    conn = get_db_connection()

    try:
        for table in TABLES_TO_EXPORT:
            try:
                df = pd.read_sql(f"SELECT * FROM {table}", conn)

                output_path = os.path.join(DB_EXPORTS, f"{table}.csv")
                df.to_csv(output_path, index=False)

                log(f"✅ Exported {table} | Rows: {len(df)}")

            except Exception as e:
                log(f"⚠ Failed to export {table} | {e}")

        log("🎯 Selected table export completed successfully")

    finally:
        close_db_connection(conn)