from db.connection import get_db_connection, close_db_connection
from config.logger import log
from pathlib import Path
import shutil
import traceback
import os

#################################################################################################
# Deletes invalid price records for a given timeframe:
# - '1wk' → keeps only Monday dates
# - '1mo' → keeps only 1st-of-month dates
#################################################################################################
def delete_invalid_timeframe_rows(timeframe: str, is_index: bool = False):
    table = "index_price_data" if is_index else "equity_price_data"

    # Timeframe-specific rules
    rules = {
        "1wk": ("strftime('%w', date) <> '1'", "non-Monday weekly"),
        "1mo": ("strftime('%d', date) <> '01'", "non-1st-day monthly"),
    }

    if timeframe not in rules:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    condition, label = rules[timeframe]

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        log(f"Deleting {label} rows from '{table}'...")

        cur.execute(f"""
            DELETE FROM {table}
            WHERE timeframe = ?
              AND {condition}
        """, (timeframe,))

        conn.commit()

        log(f"🗑️ Deleted {cur.rowcount} {label} rows from '{table}'")

    except Exception as e:
        log(f"❌ Failed to delete {label} rows from '{table}': {e}")
        traceback.print_exc()

    finally:
        close_db_connection(conn)
#################################################################################################
# Removes all CSV files from the specified directory to clean up intermediate 
# or temporary data exports.
#################################################################################################  
def delete_files_in_folder(folder_path):
    try:
        if not os.path.exists(folder_path):
            print(f"Folder does not exist: {folder_path}")
            return

        deleted = 0
        for filename in os.listdir(folder_path):
            if filename.lower().endswith(".csv"):   # match .csv or .CSV
                filepath = os.path.join(folder_path, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
                    deleted += 1

        print(f"Deleted {deleted} .csv files from: {folder_path}")
    except Exception as e:
        log(f"❌ Failed to delete .csv files from: {folder_path}: {e}")
        traceback.print_exc()

#################################################################################################
# Copies all files from a source directory to a destination directory 
# while preserving file metadata.
#################################################################################################
def copy_files(from_dir: Path, to_dir: Path):
    try:
        to_dir.mkdir(parents=True, exist_ok=True)

        for file in from_dir.iterdir():
            if file.is_file():
                shutil.copy2(file, to_dir / file.name)
        print(f"✅ Files copied from {from_dir} to {to_dir}")
    except Exception as e:
        log(f"❌ ERROR:: {e}")
        traceback.print_exc()