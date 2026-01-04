import traceback
from datetime import datetime, timedelta
import pandas as pd
from services.cleanup_service import delete_files_in_folder
from services.scanners.export_service import export_to_csv
from services.scanners.backtest_service import backtest_all_scanners
from db.connection import get_db_connection, close_db_connection
from config.paths import SCANNER_FOLDER
from config.logger import log

LOOKBACK_DAYS = 365

#################################################################################################
# APPLY HILEGA-MILEGA SCANNER LOGIC
#################################################################################################
def apply_scanner_logic(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetches weekly price data from the DB and applies weekly momentum scanner rules:
    - Close > SMA20
    - Low <= min(4 previous weeks' lows)
    - SMA20 rising compared to 2 weeks ago
    - Close >= last week close
    - Filters also applied on RSI and EMA/WMA ratios
    """
    conn = None
    try:
        conn = get_db_connection()
        sql = f"""
            WITH weekly_price AS (
                SELECT
                    ep.symbol_id,
                    ep.date,
                    ep.close,
                    ep.low,
                    AVG(ep.close) OVER (
                        PARTITION BY ep.symbol_id 
                        ORDER BY ep.date 
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS sma_20
                FROM equity_price_data ep
                WHERE ep.timeframe = '1wk'
                  AND ep.date BETWEEN '{start_date}' AND '{end_date}'
            ),
            weekly_with_lags AS (
                SELECT
                    wp.*,
                    LAG(wp.close, 1) OVER (PARTITION BY wp.symbol_id ORDER BY wp.date) AS close_1w_ago,
                    LAG(wp.sma_20, 2) OVER (PARTITION BY wp.symbol_id ORDER BY wp.date) AS sma_20_2w_ago,
                    MIN(wp.low) OVER (PARTITION BY wp.symbol_id ORDER BY wp.date ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) AS min_low_4w
                FROM weekly_price wp
            ),
            weekly_indicators AS (
                SELECT
                    wi.symbol_id,
                    wi.date,
                    wi.rsi_3,
                    wi.rsi_9,
                    wi.rsi_14,
                    wi.ema_rsi_9_3,
                    wi.wma_rsi_9_21
                FROM equity_indicators wi
                WHERE wi.timeframe = '1wk'
                  AND wi.date BETWEEN '{start_date}' AND '{end_date}'
            )
            SELECT
                p.symbol_id,
                s.symbol,
                s.name,
                p.date,
                p.close,
                p.low,
                p.sma_20,
                p.sma_20_2w_ago,
                p.close_1w_ago,
                p.min_low_4w,
                i.rsi_3,
                i.rsi_9,
                i.rsi_14,
                i.ema_rsi_9_3,
                i.wma_rsi_9_21
            FROM weekly_with_lags p
            JOIN weekly_indicators i
              ON p.symbol_id = i.symbol_id AND p.date = i.date
            JOIN equity_symbols s
              ON p.symbol_id = s.symbol_id
            WHERE p.close > p.sma_20
              AND p.low <= p.min_low_4w
              AND p.sma_20_2w_ago < p.sma_20
              AND p.close >= p.close_1w_ago
            ORDER BY p.symbol_id, p.date;
        """
        df_signals = pd.read_sql(sql, conn)

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

    finally:
        if conn:
            close_db_connection(conn)

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