import os
import pandas as pd
from datetime import datetime
from db.connection import get_db_connection, close_db_connection
from services.symbol_service import retrieve_equity_symbol
from config.logger import log
from config.paths import EXPORT_DIR  # Scanner exports folder

# -----------------------------
# Compute SMA support touches
# -----------------------------
def compute_sma_support(df: pd.DataFrame, sma_periods: list, tolerance_pct: float = 0.01):
    """
    df: must have 'close' and 'date' columns
    sma_periods: list of SMA periods to compute
    tolerance_pct: how close price must be to SMA to consider a bounce
    Returns a dict: { 'SMA_10': count, 'SMA_20': count, ... }
    """
    results = {}

    for period in sma_periods:
        df[f'SMA_{period}'] = df['close'].rolling(period).mean()
        support_count = 0

        for i in range(1, len(df)-1):
            sma_val = df[f'SMA_{period}'].iloc[i]
            close = df['close'].iloc[i]
            if pd.isna(sma_val):
                continue
            # Price within tolerance of SMA
            if abs(close - sma_val)/sma_val <= tolerance_pct:
                # Next day moved up → support
                next_close = df['close'].iloc[i+1]
                if next_close > close:
                    support_count += 1

        results[f'SMA_{period}'] = support_count

    return results

# -----------------------------
# Fetch price data for a symbol & timeframe
# -----------------------------
def fetch_price_data(symbol_id: int, timeframe: str):
    conn = get_db_connection()
    try:
        df = pd.read_sql("""
            SELECT date, close
            FROM equity_price_data
            WHERE symbol_id = ? AND timeframe = ?
            ORDER BY date ASC
        """, conn, params=(symbol_id, timeframe))
        df['date'] = pd.to_datetime(df['date'])
        return df
    finally:
        close_db_connection(conn)

# -----------------------------
# Main report generation
# -----------------------------
def generate_sma_support_report():
    try:
        conn = get_db_connection()
        log("🔹 Starting SMA support analysis...")

        os.makedirs(EXPORT_DIR, exist_ok=True)

        # 1️⃣ Get all symbols
        symbols_df = retrieve_equity_symbol("ALL",conn)
        if symbols_df.empty:
            log("❌ No symbols found in equity_symbols")
            return

        report = []
        sma_periods = list(range(10, 210, 10))
        timeframes = {'1d':'Daily', '1wk':'Weekly', '1mo':'Monthly'}

        # 2️⃣ Iterate symbols
        for _, row in symbols_df.iterrows():
            symbol_id = row['symbol_id']
            symbol_name = row['symbol']
            log(f"Processing {symbol_name}...")

            for tf, tf_name in timeframes.items():
                df_price = fetch_price_data(symbol_id, tf)
                if df_price.empty:
                    log(f"⚠ No price data for {symbol_name} | {tf_name}")
                    continue

                sma_support = compute_sma_support(df_price, sma_periods)

                # Append one row per symbol/timeframe with all SMA counts
                row_data = {
                    'symbol_id': symbol_id,
                    'symbol': symbol_name,
                    'timeframe': tf_name
                }
                row_data.update(sma_support)
                report.append(row_data)

        report_df = pd.DataFrame(report)

        # Save CSV with timestamp
        now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(EXPORT_DIR, f"sma_support_full_report_{now_str}.csv")
        report_df.to_csv(output_file, index=False)

        log(f"✅ SMA support full report saved: {output_file}")
        print(report_df.head(20))  # preview first 20 rows
    except Exception as e:
        log(f"ERROR: {e}")

    finally:
        close_db_connection(conn) 
# -----------------------------
# CLI entry point
# -----------------------------
if __name__ == "__main__":
    generate_sma_support_report()