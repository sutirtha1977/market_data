# scanner_module.py
import pandas as pd
import traceback
from datetime import datetime, timedelta
from services.cleanup_service import delete_files_in_folder
from services.scanners.export_import_service import export_to_csv
from db.connection import get_db_connection, close_db_connection
from services.scanners.backtest_service import backtest_all_scanners
from config.paths import SCANNER_FOLDER
from services.symbol_service import retrieve_equity_symbol
from services.scanners.data_service import get_base_data
from config.logger import log

LOOKBACK_DAYS = 365

#################################################################################################
# PLACEHOLDER FOR SCANNER LOGIC
#################################################################################################
def apply_scanner_logic(df):
    """
    Example placeholder for scanner logic.
    Returns a DataFrame of signals.
    """
    if df.empty:
        return df

    # Example: simple breakout above previous high
    df['prev_high'] = df['high'].shift(1)
    df['signal'] = df['close'] > df['prev_high']
    signals = df[df['signal']].copy()
    signals = signals[['symbol_id', 'symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'signal']]
    return signals

#################################################################################################
# MAIN SCANNER FUNCTION
#################################################################################################
def run_scanner_export(lookback_days=LOOKBACK_DAYS):
    """
    Runs the scanner for all symbols using get_base_data and exports results to SCANNER_FOLDER.
    """
    log("🧹 Clearing scanner folder...")
    delete_files_in_folder(SCANNER_FOLDER)

    conn = get_db_connection()
    try:
        log("🔍 Fetching base data...")
        df_base = get_base_data(lookback_days=lookback_days)
        if df_base.empty:
            log("❌ No base data found")
            return None

        all_signals = []

        # Group by symbol_id
        for symbol_id, df_sym in df_base.groupby('symbol_id'):
            symbol = df_sym['symbol'].iloc[0]
            log(f"Processing symbol: {symbol} (ID: {symbol_id})")

            try:
                signals = apply_scanner_logic(df_sym)
                if not signals.empty:
                    signals['timeframe'] = '1d'
                    all_signals.append(signals)

            except Exception as e_sym:
                log(f"⚠ Error processing {symbol}: {e_sym}")

        if not all_signals:
            log("❌ No signals generated")
            return None

        final_df = pd.concat(all_signals, ignore_index=True)
        path = export_to_csv(final_df, SCANNER_FOLDER, "scanner_signals")
        log(f"✅ Scanner results saved to: {path}")
        return final_df

    finally:
        close_db_connection(conn)


#################################################################################################
# MULTI-YEAR SCANNER FUNCTION
#################################################################################################
def scanner_play_multi_years(start_year: str, lookback_years: int):
    """
    Runs the scanner for multiple years, fetching data year-by-year using get_base_data.
    After scanning all years, it runs the backtest.
    """
    try:
        # Clean scanner folder before starting
        print(f"===== DELETE FILES FROM SCANNER FOLDER STARTED =====")
        delete_files_in_folder(SCANNER_FOLDER)
        print(f"===== DELETE FILES FROM SCANNER FOLDER FINISHED =====")

        # Convert start_year to int safely
        try:
            start_year_int = int(start_year)
        except ValueError:
            print(f"❌ Invalid start year '{start_year}', defaulting to 2025")
            start_year_int = 2025

        for i in range(lookback_years):
            year = start_year_int - i
            # Last day of the year
            end_date = datetime(year, 12, 31).strftime("%Y-%m-%d")
            print(f"\n🔹 Running scanner for year {year} (end date {end_date})")

            # Fetch base data for the year up to end_date
            df_year = get_base_data(lookback_days=LOOKBACK_DAYS, start_date=end_date)
            if df_year.empty:
                print(f"⚠ No data found for year {year}")
                continue

            all_signals = []

            for symbol_id, df_sym in df_year.groupby('symbol_id'):
                symbol = df_sym['symbol'].iloc[0]
                try:
                    signals = apply_scanner_logic(df_sym)
                    if not signals.empty:
                        signals['timeframe'] = '1d'
                        signals['symbol_id'] = symbol_id
                        signals['symbol'] = symbol
                        all_signals.append(signals)
                except Exception as e_sym:
                    log(f"⚠ Error processing {symbol} for year {year}: {e_sym}")

            if all_signals:
                final_df = pd.concat(all_signals, ignore_index=True)
                export_to_csv(final_df, SCANNER_FOLDER, f"scanner_signals_{year}")
                print(f"✅ Completed for {year} | Rows found: {len(final_df)}")

        summary, trades = backtest_all_scanners()
        print(summary)
        return summary, trades

    except Exception as e:
        print(f"❌ ERROR | {e}")
        traceback.print_exc()
#################################################################################################
# MAIN SCANNER FUNCTION
#################################################################################################
def run_scanner_export(lookback_days=LOOKBACK_DAYS):
    """
    Runs the scanner for all symbols using get_base_data and exports results to SCANNER_FOLDER.
    """
    log("🧹 Clearing scanner folder...")
    delete_files_in_folder(SCANNER_FOLDER)

    conn = get_db_connection()
    try:
        log("🔍 Fetching base data...")
        df_base = get_base_data(lookback_days=lookback_days)
        if df_base.empty:
            log("❌ No base data found")
            return None

        all_signals = []

        # Group by symbol_id and timeframe if needed (here daily data is already fetched)
        for symbol_id, df_sym in df_base.groupby('symbol_id'):
            symbol = df_sym['symbol'].iloc[0]
            log(f"Processing symbol: {symbol} (ID: {symbol_id})")

            try:
                signals = apply_scanner_logic(df_sym)
                if not signals.empty:
                    signals['timeframe'] = '1d'
                    all_signals.append(signals)

            except Exception as e_sym:
                log(f"⚠ Error processing {symbol}: {e_sym}")

        if not all_signals:
            log("❌ No signals generated")
            return None

        final_df = pd.concat(all_signals, ignore_index=True)
        path = export_to_csv(final_df, SCANNER_FOLDER, "scanner_signals")
        log(f"✅ Scanner results saved to: {path}")
        return final_df

    finally:
        close_db_connection(conn)