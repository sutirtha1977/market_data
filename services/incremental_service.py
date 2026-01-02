import os
import traceback
import pandas as pd
from config.logger import log
from services.cleanup_service import delete_invalid_timeframe_rows, delete_files_in_folder, copy_files
from services.symbol_service import get_latest_equity_date
from services.yahoo_service import (
    download_equity_yahoo_incr_data_all_timeframes, 
    download_index_yahoo_incr_data_all_timeframes
)
from services.equity_service import import_equity_csv_to_db
from services.index_service import import_index_csv_to_db
from config.paths import YAHOO_EQUITY_DIR, YAHOO_INDEX_DIR, BHAVCOPY_DIR, BHAVCOPY_DIR_DB
from services.bhavcopy_loader import download_missing_bhavcopies, update_equity_delv_pct_from_bhavcopy
from config.paths import FREQUENCIES

def incr_yahoo_bhavcopy_download(symbol):
    try:
        log(f"===== FETCH DAILY LATEST DATE STARTED =====")
        print(f"===== FETCH DAILY LATEST DATE STARTED =====")
        latest_dt = get_latest_equity_date()
        print(f"DAILY LATEST DATE IS: {latest_dt} =====")
        log(f"DAILY LATEST DATE IS: {latest_dt} =====")
        # -------------------------
        # EQUITY DOWNLOAD
        # -------------------------
        log(f"===== YAHOO EQUITY DOWNLOAD STARTED =====")
        print(f"===== YAHOO EQUITY DOWNLOAD STARTED =====")
        download_equity_yahoo_incr_data_all_timeframes(latest_dt=latest_dt, symbol=symbol)
        print(f"===== YAHOO EQUITY DOWNLOAD FINISHED =====")
        log(f"===== YAHOO EQUITY DOWNLOAD FINISHED =====")

        log(f"===== UPDATE EQUITY CSV TO DB STARTED =====")
        print(f"===== UPDATE EQUITY CSV TO DB STARTED =====")
        import_equity_csv_to_db()
        print(f"===== UPDATE EQUITY CSV TO DB FINISHED =====")
        log(f"===== UPDATE EQUITY CSV TO DB FINISHED =====")

        log(f"===== DELETE INVALID ROWS FOR WEEK & MONTH STARTED =====")
        print(f"===== DELETE INVALID ROWS FOR WEEK & MONTH STARTED =====")
        delete_invalid_timeframe_rows("1wk")
        delete_invalid_timeframe_rows("1mo")
        print(f"===== DELETE INVALID ROWS FOR WEEK & MONTH FINISHED =====")
        log(f"===== DELETE INVALID ROWS FOR WEEK & MONTH FINISHED =====")
        
        # -------------------------
        # INDEX (ONLY IF ALL)
        # -------------------------
        if symbol == "ALL":
            log(f"===== YAHOO INDEX DOWNLOAD STARTED =====")
            print(f"===== YAHOO INDEX DOWNLOAD STARTED =====")
            download_index_yahoo_incr_data_all_timeframes(latest_dt)
            print(f"===== YAHOO INDEX DOWNLOAD FINISHED =====")
            log(f"===== YAHOO INDEX DOWNLOAD FINISHED =====")

            log(f"===== UPDATE INDEX CSV TO DB STARTED =====")
            print(f"===== UPDATE INDEX CSV TO DB STARTED =====")
            import_index_csv_to_db()
            print(f"===== UPDATE INDEX CSV TO DB FINISHED =====")
            log(f"===== UPDATE INDEX CSV TO DB FINISHED =====")

            log(f"===== DELETE INVALID ROWS FOR WEEK & MONTH STARTED =====")
            print(f"===== DELETE INVALID ROWS FOR WEEK & MONTH STARTED =====")
            delete_invalid_timeframe_rows("1wk", is_index=True)
            delete_invalid_timeframe_rows("1mo", is_index=True)
            print(f"===== DELETE INVALID ROWS FOR WEEK & MONTH FINISHED =====")
            log(f"===== DELETE INVALID ROWS FOR WEEK & MONTH FINISHED =====")
        # -------------------------
        # BHAVCOPY
        # -------------------------    
        log(f"===== BHAVCOPY DOWNLOAD STARTED =====")
        print(f"===== BHAVCOPY DOWNLOAD STARTED =====")
        download_missing_bhavcopies(latest_dt)
        print(f"===== BHAVCOPY DOWNLOAD FINISHED =====")
        log(f"===== BHAVCOPY DOWNLOAD FINISHED =====")

        log(f"===== UPDATE DELIVERY PERCENTAGE FROM BHAVCOPY STARTED =====")
        print(f"===== UPDATE DELIVERY PERCENTAGE FROM BHAVCOPY STARTED =====")
        update_equity_delv_pct_from_bhavcopy(symbol="ALL")
        print(f"===== UPDATE DELIVERY PERCENTAGE FROM BHAVCOPY FINISHED =====")
        log(f"===== UPDATE DELIVERY PERCENTAGE FROM BHAVCOPY FINISHED =====")
        # -------------------------
        # CLEANUP FILES
        # -------------------------
        log(f"===== DELETE FILES FROM EQUITY AND INDEX FOLDERS STARTED =====")
        print(f"===== DELETE FILES FROM EQUITY AND INDEX FOLDERS STARTED =====")
        for timeframe in FREQUENCIES:
            delete_files_in_folder(os.path.join(YAHOO_EQUITY_DIR, timeframe))      
        # 👉 delete index files ONLY if symbol == ALL
        if symbol == "ALL":
            for timeframe in FREQUENCIES:
                delete_files_in_folder(os.path.join(YAHOO_INDEX_DIR, timeframe))
        print(f"===== DELETE FILES FROM EQUITY AND INDEX FOLDERS FINISHED =====")
        log(f"===== DELETE FILES FROM EQUITY AND INDEX FOLDERS FINISHED =====")
        # -------------------------
        # BHAVCOPY FILE MOVE
        # -------------------------
        log(f"===== COPY BHAVCOPY FILES STARTED =====")
        print(f"===== COPY BHAVCOPY FILES STARTED =====")
        copy_files(BHAVCOPY_DIR,BHAVCOPY_DIR_DB)
        log(f"===== COPY BHAVCOPY FILES FINISHED =====")
        print(f"===== COPY BHAVCOPY FILES FINISHED =====")
        
        log(f"===== DELETE FILES FROM BHAVCOPY FOLDERS STARTED =====")
        print(f"===== DELETE FILES FROM BHAVCOPY FOLDERS STARTED =====")
        delete_files_in_folder(BHAVCOPY_DIR)
        log(f"===== DELETE FILES FROM BHAVCOPY FOLDERS STARTED =====")
        print(f"===== DELETE FILES FROM BHAVCOPY FOLDERS STARTED =====")
    except Exception as e:
        log(f"ERROR: {e}")
        traceback.print_exc()