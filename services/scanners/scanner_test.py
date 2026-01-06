import pandas as pd
from db.connection import get_db_connection
from services.scanners.export_import_service import export_to_csv
from services.cleanup_service import delete_files_in_folder
from config.paths import SCANNER_FOLDER
from config.logger import log


# --------------------------------------------------
# STEP 1: LOAD DAILY DATA (2025)
# --------------------------------------------------
def get_daily_2025(conn) -> pd.DataFrame:
    sql = """
        SELECT
            ei.symbol_id,
            s.symbol,
            ei.date,
            ei.rsi_3,
            ei.rsi_9,
            ei.rsi_14,
            ei.ema_rsi_9_3,
            ei.wma_rsi_9_21
        FROM equity_indicators ei
        JOIN equity_symbols s
          ON ei.symbol_id = s.symbol_id
        WHERE ei.timeframe = '1d'
          AND ei.date BETWEEN '2025-01-01' AND '2025-12-31'
          AND ei.is_final = 1
    """
    return pd.read_sql(sql, conn, parse_dates=["date"])


# --------------------------------------------------
# STEP 2: LOAD MONTHLY OUTCOMES
# --------------------------------------------------
def get_monthly_outcomes(conn) -> pd.DataFrame:
    sql = """
        SELECT
            symbol_id,
            date AS month_end,
            pct_price_change
        FROM equity_indicators
        WHERE timeframe = '1mo'
          AND date BETWEEN '2025-01-01' AND '2025-12-31'
          AND is_final = 1
    """
    return pd.read_sql(sql, conn, parse_dates=["month_end"])


# --------------------------------------------------
# STEP 3: FEATURE ENGINEERING
# --------------------------------------------------
def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["rsi3_rsi9"] = df["rsi_3"] / df["rsi_9"]

    df["order_lock"] = (
        (df["rsi_3"] > df["rsi_9"]) &
        (df["rsi_9"] > df["ema_rsi_9_3"]) &
        (df["ema_rsi_9_3"] > df["wma_rsi_9_21"])
    )

    df["acceleration"] = (
        (df["rsi_3"] - df["rsi_9"]) >
        (df["rsi_9"] - df["ema_rsi_9_3"])
    )

    return df


# --------------------------------------------------
# STEP 4: MAP DAILY → NEXT MONTH OUTCOME
# --------------------------------------------------
def map_future_success(daily: pd.DataFrame, monthly: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    monthly = monthly.copy()

    monthly["month"] = monthly["month_end"].dt.to_period("M")
    daily["month"] = daily["date"].dt.to_period("M") + 1

    merged = daily.merge(
        monthly[["symbol_id", "month", "pct_price_change"]],
        on=["symbol_id", "month"],
        how="left"
    )

    merged["success"] = (merged["pct_price_change"] >= 50).astype(int)
    merged.drop(columns=["month"], inplace=True)

    return merged


# --------------------------------------------------
# STEP 5: RUN 2025 PROBABILISTIC SCANNER
# --------------------------------------------------
def run_probabilistic_scanner():
    log("🚀 Running 2025 probabilistic scanner")
    # ---------------- CLEAN SCANNER FOLDER ----------------
    print(f"===== DELETE FILES FROM SCANNER FOLDER STARTED =====")
    delete_files_in_folder(SCANNER_FOLDER)
    print(f"===== DELETE FILES FROM SCANNER FOLDER FINISHED =====")
    conn = get_db_connection()
    try:
        daily = get_daily_2025(conn)
        log(f"📥 Daily rows loaded: {len(daily)}")

        if daily.empty:
            log("❌ No daily data")
            return

        monthly = get_monthly_outcomes(conn)
        log(f"📥 Monthly rows loaded: {len(monthly)}")

        df = add_features(daily)
        df = map_future_success(df, monthly)

        df = df.sort_values(["date", "symbol"])

        export_to_csv(df, SCANNER_FOLDER, "probabilistic_scanner_2025")

        log("✅ 2025 probabilistic dataset created")

    except Exception as e:
        log(f"❌ Scanner failed | {e}")

    finally:
        conn.close()