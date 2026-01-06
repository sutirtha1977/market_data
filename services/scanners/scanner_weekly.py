import traceback
from datetime import datetime, timedelta
import pandas as pd
from services.cleanup_service import delete_files_in_folder
from services.scanners.export_import_service import export_to_csv
# from services.scanners.backtest_service import backtest_all_scanners
from services.scanners.data_service import get_base_data_weekly
from db.connection import get_db_connection, close_db_connection
from config.paths import SCANNER_FOLDER
from config.logger import log

LOOKBACK_DAYS = 365

#################################################################################################
# APPLY SCANNER LOGIC
#################################################################################################
def apply_scanner_logic(start_date: str, end_date: str) -> pd.DataFrame:
    try:
        df_signals = get_base_data_weekly(start_date=start_date, end_date=end_date)
        # Filter as per original logic
        df_filtered = df_signals[
            (df_signals['close'] >= 100) &
            (df_signals['rsi_3'] / df_signals['rsi_9'] >= 1.15) &
            (df_signals['rsi_9'] / df_signals['ema_rsi_9_3'] >= 1.04) &
            (df_signals['ema_rsi_9_3'] / df_signals['wma_rsi_9_21'] >= 1) &
            (df_signals['rsi_3'] > 50)
        ].sort_values(['date','symbol'], ascending=[False, True])

        return df_filtered  # Return filtered signals

    except Exception as e:
        log(f"❌ Error fetching weekly scanner data | {e}")
        traceback.print_exc()
        return pd.DataFrame()


#################################################################################################
# Runs the weekly momentum scanner for a given date range, exports results to CSV, 
# and returns the signals as a DataFrame.
#################################################################################################
def run_scanner_weekly(start_date: str | None = None) -> pd.DataFrame:
    try:
        # -------------------- CALCULATE DATES --------------------
        today = datetime.today()
        end_date_dt = today
        start_date_dt = today - timedelta(days=LOOKBACK_DAYS)

        # Override with passed start_date if provided
        if start_date:
            start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")

        # Format dates as strings for SQL
        start_date_str = start_date_dt.strftime("%Y-%m-%d")
        end_date_str = end_date_dt.strftime("%Y-%m-%d")

        # -------------------- CLEAN SCANNER FOLDER --------------------
        log("🧹 Clearing scanner folder...")
        delete_files_in_folder(SCANNER_FOLDER)

        # -------------------- RUN SCANNER --------------------
        log(f"🔍 Running weekly scanner from {start_date_str} to {end_date_str}")
        df_signals = apply_scanner_logic(start_date_str, end_date_str)

        if df_signals.empty:
            log(f"⚠ No weekly momentum signals found for {start_date_str} to {end_date_str}")
            return pd.DataFrame()

        # -------------------- EXPORT RESULTS --------------------
        path = export_to_csv(df_signals, SCANNER_FOLDER, "WEEKLY")
        log(f"✅ Hilega-Milega scanner results saved to: {path}")

        return df_signals

    except Exception as e:
        log(f"❌ run_scanner failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()