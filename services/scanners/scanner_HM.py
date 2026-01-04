# scanner_hilega_milega_module.py
import traceback
import pandas as pd
from services.cleanup_service import delete_files_in_folder
from services.scanners.export_service import export_to_csv
from db.connection import get_db_connection, close_db_connection
from services.scanners.data_service import get_base_data
from config.paths import SCANNER_FOLDER
from config.logger import log

LOOKBACK_DAYS = 365

#################################################################################################
# APPLY SCANNER LOGIC
#################################################################################################
def apply_hilega_milega_logic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies the Hilega-Milega scanner rules to base data.
    """
    if df.empty:
        return df

    # Filter as per original logic
    df_filtered = df[
        (df['adj_close'] >= 100) &
        (df['adj_close'] < df['sma_20']) &
        (df['rsi_3'] / df['rsi_9'] >= 1.15) &
        (df['rsi_9'] / df['ema_rsi_9_3'] >= 1.04) &
        (df['ema_rsi_9_3'] / df['wma_rsi_9_21'] >= 1) &
        (df['rsi_3'] < 60) &
        (df['rsi_3_weekly'] > 50) &
        (df['rsi_3_monthly'] > 50) &
        (df['pct_price_change'] <= 5)
    ].sort_values(['date','symbol'], ascending=[False, True])

    return df_filtered

#################################################################################################
# MAIN SCANNER FUNCTION
#################################################################################################
def run_scanner_hilega_milega(start_date: str | None = None) -> pd.DataFrame:
    """
    Runs the Hilega-Milega scanner for all symbols using get_base_data.
    Exports the results to SCANNER_FOLDER.
    """
    try:
        log("🧹 Clearing scanner folder...")
        delete_files_in_folder(SCANNER_FOLDER)

        log("🔍 Fetching base data...")
        df_base = get_base_data(lookback_days=LOOKBACK_DAYS, start_date=start_date)

        if df_base is None or df_base.empty:
            log(f"❌ No base data found for end date: {start_date}")
            return pd.DataFrame()

        # Ensure required columns exist
        required_cols = [
            'adj_close', 'rsi_3', 'rsi_9', 'ema_rsi_9_3', 
            'wma_rsi_9_21', 'rsi_3_weekly', 'rsi_3_monthly', 'sma_20', 'pct_price_change'
        ]
        missing_cols = [c for c in required_cols if c not in df_base.columns]
        if missing_cols:
            log(f"❌ Missing required columns in base data: {missing_cols}")
            return pd.DataFrame()

        log("⚙️ Applying Hilega-Milega scanner logic...")
        df_signals = apply_hilega_milega_logic(df_base)

        if df_signals.empty:
            log(f"⚠ No stocks met scanner criteria for end date: {start_date}")
            return pd.DataFrame()

        path = export_to_csv(df_signals, SCANNER_FOLDER, "HM")
        log(f"✅ Hilega-Milega scanner results saved to: {path}")

        return df_signals

    except Exception as e:
        log(f"❌ scanner_hilega_milega failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()