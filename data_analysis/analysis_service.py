import pandas as pd
from datetime import timedelta
import traceback

from db.connection import get_db_connection, close_db_connection
from services.scanners.export_import_service import export_to_csv
from services.cleanup_service import delete_files_in_folder
from config.paths import ANALYSIS_FOLDER
from config.logger import log


# --------------------------------------------------
# STEP 1: MONTHLY BREAKOUT EVENTS WITH PREV MONTH CLOSE
# --------------------------------------------------
def get_monthly_breakouts(conn) -> pd.DataFrame:
    try:
        log("📊 Fetching monthly breakout events (≥50%)")

        sql = """
            WITH monthly_price AS (
                SELECT
                    ep.symbol_id,
                    ep.date,
                    ep.close,
                    LAG(ep.close) OVER (
                        PARTITION BY ep.symbol_id
                        ORDER BY ep.date
                    ) AS prev_month_close
                FROM equity_price_data ep
                WHERE ep.timeframe = '1mo'
            )
            SELECT
                ei.symbol_id,
                s.symbol,
                ei.date AS month_end_date,
                ei.pct_price_change,
                mp.prev_month_close
            FROM equity_indicators ei
            JOIN equity_symbols s
                ON ei.symbol_id = s.symbol_id
            JOIN monthly_price mp
                ON ei.symbol_id = mp.symbol_id
               AND ei.date = mp.date
            WHERE ei.timeframe = '1mo'
              AND ei.date BETWEEN '2025-01-01' AND '2025-12-31'
              AND ei.pct_price_change >= 50
              AND mp.prev_month_close IS NOT NULL
            ORDER BY ei.date, ei.symbol_id
        """

        df = pd.read_sql(sql, conn, parse_dates=["month_end_date"])
        log(f"✅ Monthly breakouts found: {len(df)}")
        return df

    except Exception as e:
        log(f"❌ get_monthly_breakouts failed | {e}")
        return pd.DataFrame()


# --------------------------------------------------
# STEP 2: FIND FIRST DAILY CLOSE ABOVE PREV MONTH CLOSE
# --------------------------------------------------
def find_signal_day(conn, symbol_id, month_end_date, prev_month_close):
    try:
        month_end_str = month_end_date.strftime("%Y-%m-%d")

        sql = """
            SELECT
                date,
                close
            FROM equity_price_data
            WHERE symbol_id = ?
              AND timeframe = '1d'
              AND date >= date(?, 'start of month')
              AND date <= ?
              AND close > ?
            ORDER BY date ASC
            LIMIT 1
        """

        df = pd.read_sql(
            sql,
            conn,
            params=(
                symbol_id,
                month_end_str,
                month_end_str,
                float(prev_month_close)
            ),
            parse_dates=["date"]
        )

        return df.iloc[0] if not df.empty else None

    except Exception as e:
        log(f"⚠ Failed finding signal day | symbol_id={symbol_id} | {e}")
        return None


# --------------------------------------------------
# STEP 3: FETCH INDICATORS (LATEST ≤ SIGNAL DATE)
# --------------------------------------------------
def fetch_indicators(conn, symbol_id, signal_date, timeframe):
    signal_date_str = signal_date.strftime("%Y-%m-%d")

    sql = """
        SELECT
            rsi_3,
            rsi_9,
            rsi_14,
            ema_rsi_9_3,
            wma_rsi_9_21,
            date AS indicator_date
        FROM equity_indicators
        WHERE symbol_id = ?
          AND timeframe = ?
          AND date <= ?
          AND is_final = 1
        ORDER BY date DESC
        LIMIT 1
    """

    df = pd.read_sql(
        sql,
        conn,
        params=(symbol_id, timeframe, signal_date_str)
    )

    return df.iloc[0].to_dict() if not df.empty else {}


# --------------------------------------------------
# STEP 4: BUILD FINAL ANALYSIS DATASET
# --------------------------------------------------
def build_signal_dataset(conn, breakouts: pd.DataFrame) -> pd.DataFrame:
    records = []

    for _, row in breakouts.iterrows():
        signal = find_signal_day(
            conn,
            row.symbol_id,
            row.month_end_date,
            row.prev_month_close
        )

        if signal is None:
            log(f"⏭ No signal day | {row.symbol} | {row.month_end_date:%Y-%m}")
            continue

        signal_date = signal["date"]

        daily  = fetch_indicators(conn, row.symbol_id, signal_date, "1d")
        weekly = fetch_indicators(conn, row.symbol_id, signal_date, "1wk")
        monthly = fetch_indicators(conn, row.symbol_id, signal_date, "1mo")

        record = {
            "symbol_id": row.symbol_id,
            "symbol": row.symbol,
            "month_end_date": row.month_end_date,
            "signal_date": signal_date,
            "prev_month_close": row.prev_month_close,
            "signal_close": signal["close"],

            # DAILY
            "d_rsi_3": daily.get("rsi_3"),
            "d_rsi_9": daily.get("rsi_9"),
            "d_rsi_14": daily.get("rsi_14"),
            "d_ema_rsi_9_3": daily.get("ema_rsi_9_3"),
            "d_wma_rsi_9_21": daily.get("wma_rsi_9_21"),

            # WEEKLY (max available till date)
            "w_rsi_3": weekly.get("rsi_3"),
            "w_rsi_9": weekly.get("rsi_9"),
            "w_rsi_14": weekly.get("rsi_14"),

            # MONTHLY (max available till date)
            "m_rsi_3": monthly.get("rsi_3"),
            "m_rsi_9": monthly.get("rsi_9"),
            "m_rsi_14": monthly.get("rsi_14"),
        }

        records.append(record)

    return pd.DataFrame(records)


# --------------------------------------------------
# MAIN DRIVER
# --------------------------------------------------
def run_research():
    conn = None
    try:
        log("🧹 Clearing analysis folder...")
        delete_files_in_folder(ANALYSIS_FOLDER)

        log("🚀 Starting breakout signal research")
        conn = get_db_connection()

        breakouts = get_monthly_breakouts(conn)
        if breakouts.empty:
            log("❌ No breakout stocks found")
            return

        df_signals = build_signal_dataset(conn, breakouts)

        if df_signals.empty:
            log("❌ No valid signal records created")
            return

        export_to_csv(df_signals, ANALYSIS_FOLDER, "probabilistic_signal_base")
        log(f"✅ Analysis complete | Records: {len(df_signals)}")

    except Exception as e:
        log(f"❌ run_research crashed | {e}")
        traceback.print_exc()

    finally:
        if conn:
            close_db_connection(conn)
            log("🔒 DB connection closed")
