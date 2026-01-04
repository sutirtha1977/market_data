# scanner_discount_zone.py
import traceback
from datetime import datetime
import pandas as pd
from db.connection import get_db_connection, close_db_connection
from services.cleanup_service import delete_files_in_folder
from services.scanners.export_service import export_to_csv
from config.paths import SCANNER_FOLDER
from config.logger import log

PRICE_THRESHOLD_PCT = 2  # % above zone low to trigger signal

#################################################################################################
# RUN DISCOUNT ZONE SCANNER USING SINGLE SQL
#################################################################################################
def run_discount_zone_scanner(year: int = 2025):
    """
    Runs the Discount Zone scanner using SQL.
    Returns all signals where price approaches a Discount Zone.
    """
    conn = None
    try:
        log("🧹 Clearing scanner folder...")
        delete_files_in_folder(SCANNER_FOLDER)

        conn = get_db_connection()

        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        log(f"🔍 Running Discount Zone scan for year {year}...")

        # ---------------- SINGLE SQL TO FETCH SIGNALS ----------------
        sql = f"""
        WITH zone_thresholds AS (
            SELECT
                symbol_id,
                timeframe,
                date AS zone_date,
                low AS zone_low,
                high AS zone_high,
                low * (1 + {PRICE_THRESHOLD_PCT}/100.0) AS threshold
            FROM smc_structures
            WHERE structure_type = 'Discount_Zone'
              AND date BETWEEN '{start_date}' AND '{end_date}'
        )
        SELECT
            p.symbol_id,
            p.timeframe,
            p.date AS price_date,
            p.close AS price_close,
            z.zone_date,
            z.zone_low,
            z.zone_high,
            'Buy_Discount_Zone' AS signal
        FROM equity_price_data p
        JOIN zone_thresholds z
          ON p.symbol_id = z.symbol_id
         AND p.timeframe = z.timeframe
         AND p.date >= z.zone_date
        WHERE p.low <= z.threshold
          AND p.date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY p.symbol_id, p.timeframe, p.date;
        """
        print(sql)
        # df_signals = pd.read_sql(sql, conn)
        # if df_signals.empty:
        #     log(f"⚠ No Discount Zone signals found for {year}")
        #     return pd.DataFrame()

        # # ---------------- EXPORT RESULTS ----------------
        # path = export_to_csv(df_signals, SCANNER_FOLDER, f"Discount_Zone_{year}")
        # log(f"✅ Discount Zone scanner completed. Results saved to: {path}")

        # return df_signals

    except Exception as e:
        log(f"❌ Discount Zone scanner failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()

    finally:
        if conn:
            close_db_connection(conn)