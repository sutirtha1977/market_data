from db.connection import get_db_connection, close_db_connection
from config.logger import log
from config.paths import FREQUENCIES
from services.indicators_helper import (
    calculate_rsi_series, calculate_bollinger, 
    calculate_atr, calculate_macd, 
    calculate_supertrend, calculate_ema, calculate_wma
)
from db.sql import SQL_INSERT
import pandas as pd
import traceback
import time
import sys

#################################################################################################
# Calculate Indicators
#################################################################################################
def calculate_indicators(df, latest_only=False):
    try:
        # ---------------- SMA ----------------
        df["sma_20"] = df["adj_close"].rolling(20).mean().round(2)
        df["sma_50"] = df["adj_close"].rolling(50).mean().round(2)
        df["sma_200"] = df["adj_close"].rolling(200).mean().round(2)
        # ---------------- RSI ----------------
        df["rsi_3"] = calculate_rsi_series(df["close"], 3)
        df["rsi_9"] = calculate_rsi_series(df["close"], 9)
        df["rsi_14"] = calculate_rsi_series(df["close"], 14)
        # ---------------- Other Indicators ----------------
        df["ema_rsi_9_3"] = calculate_ema(df["rsi_9"], 3)
        df["wma_rsi_9_21"] = calculate_wma(df["rsi_9"], 21)
        # --------------- Bollinger Bands, ATR, Supertrend, MACD ----------------
        df["bb_upper"], df["bb_middle"], df["bb_lower"] = calculate_bollinger(df["close"])
        df["atr_14"] = calculate_atr(df)
        df["supertrend"], df["supertrend_dir"] = calculate_supertrend(df)
        df["macd"], df["macd_signal"] = calculate_macd(df["close"])
        # # --------------- Percentage Price Change ----------------
        df["pct_price_change"] = (df["adj_close"].pct_change() * 100).round(2)
        # --------------- Percentage Price Change ----------------
        # df["pct_price_change"] = (
        #     df["adj_close"].pct_change(fill_method=None).mul(100).round(2)
        # )

        # ---- Return only last row if requested ----
        if latest_only:
            return df.iloc[[-1]].reset_index(drop=True)

        return df

    except Exception as e:
        log(f"{sys._getframe().f_code.co_name} FAILED | {e}")
        traceback.print_exc()
        return df  # return original df on failure
#################################################################################################
# Update Indicators
#################################################################################################
def refresh_indicators():
    # ------------------------------
    # PART 1: EQUITIES
    # ------------------------------
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        table_symbols   = "equity_symbols"
        price_table     = "equity_price_data"
        indicator_table = "equity_indicators"
        col_id          = "symbol_id"

        # Load equity symbol IDs
        cur.execute(f"SELECT {col_id} FROM {table_symbols}")
        symbol_ids = [row[0] for row in cur.fetchall()]
        print(f"\n🔢 Loaded {len(symbol_ids)} equities")

        insert_sql_equity = f"""{SQL_INSERT['equity']}""".format(
            indicator_table=indicator_table,
            col_id=col_id
        )
        for timeframe in FREQUENCIES:
            print(f"\n⏳ Processing EQUITIES timeframe: {timeframe}")
            tf_start_time = time.time()
            inserted_rows = 0
            processed_symbols = 0

            for idx, symbol_id in enumerate(symbol_ids, start=1):
                if idx <= 3 or idx % 250 == 0:
                    print(f"  → {idx}/{len(symbol_ids)} symbols...", flush=True)

                try:
                    df = pd.read_sql(f"""
                        SELECT date, open, high, low, close, adj_close, is_final
                        FROM {price_table}
                        WHERE {col_id}=? AND timeframe=?
                        ORDER BY date
                    """, conn, params=(symbol_id, timeframe))

                    if df.empty:
                        continue
                    
                    calculate_indicators(df, latest_only=False)

                    records = [
                        (
                            symbol_id, timeframe, row["date"], row["is_final"],
                            row["sma_20"], row["sma_50"], row["sma_200"],
                            row["rsi_3"], row["rsi_9"], row["rsi_14"],
                            row["bb_upper"], row["bb_middle"], row["bb_lower"],
                            row["atr_14"], row["supertrend"], row["supertrend_dir"],
                            row["ema_rsi_9_3"], row["wma_rsi_9_21"], row["pct_price_change"],
                            row["macd"], row["macd_signal"]
                        )
                        for _, row in df.iterrows()
                    ]

                    cur.executemany(insert_sql_equity, records)
                    conn.commit()
                    inserted_rows += len(records)
                    processed_symbols += 1

                except Exception as e:
                    print(f"❌ ERROR EQUITY {symbol_id} T={timeframe} | {e}")
                    traceback.print_exc()

            print(f"  ✔ EQUITIES {timeframe} DONE | {processed_symbols} symbols | {inserted_rows} rows | {time.time()-tf_start_time:.1f}s")

        print("🎉 EQUITIES indicators refreshed successfully!")

    except Exception as e:
        print(f"❌ CRITICAL FAILURE — EQUITIES | {e}")
        traceback.print_exc()

    # ------------------------------
    # PART 2: INDICES
    # ------------------------------
    try:
        cur = conn.cursor()
        table_symbols   = "index_symbols"
        price_table     = "index_price_data"
        indicator_table = "index_indicators"
        col_id          = "index_id"

        cur.execute(f"SELECT {col_id} FROM {table_symbols}")
        symbol_ids = [row[0] for row in cur.fetchall()]
        print(f"\n🔢 Loaded {len(symbol_ids)} indices")

        insert_sql_index = f"""{SQL_INSERT['index']}""".format(
            indicator_table=indicator_table,
            col_id=col_id
        )
        for timeframe in FREQUENCIES:
            print(f"\n⏳ Processing INDICES timeframe: {timeframe}")
            tf_start_time = time.time()
            inserted_rows = 0
            processed_symbols = 0

            for idx, symbol_id in enumerate(symbol_ids, start=1):
                if idx <= 3 or idx % 250 == 0:
                    print(f"  → {idx}/{len(symbol_ids)} symbols...", flush=True)

                try:
                    df = pd.read_sql(f"""
                        SELECT date, open, high, low, close, adj_close
                        FROM {price_table}
                        WHERE {col_id}=? AND timeframe=?
                        ORDER BY date
                    """, conn, params=(symbol_id, timeframe))

                    if df.empty:
                        continue

                    calculate_indicators(df, latest_only=False)

                    records = [
                        (
                            symbol_id, timeframe, row["date"],
                            row["sma_20"], row["sma_50"], row["sma_200"],
                            row["rsi_3"], row["rsi_9"], row["rsi_14"],
                            row["bb_upper"], row["bb_middle"], row["bb_lower"],
                            row["atr_14"], row["supertrend"], row["supertrend_dir"],
                            row["ema_rsi_9_3"], row["wma_rsi_9_21"], row["pct_price_change"],
                            row["macd"], row["macd_signal"]
                        )
                        for _, row in df.iterrows()
                    ]

                    cur.executemany(insert_sql_index, records)
                    conn.commit()
                    inserted_rows += len(records)
                    processed_symbols += 1

                except Exception as e:
                    print(f"❌ ERROR INDEX {symbol_id} T={timeframe} | {e}")
                    traceback.print_exc()

            print(f"  ✔ INDICES {timeframe} DONE | {processed_symbols} symbols | {inserted_rows} rows | {time.time()-tf_start_time:.1f}s")

        print("🎉 INDICES indicators refreshed successfully!")

    except Exception as e:
        print(f"❌ CRITICAL FAILURE — INDICES | {e}")
        traceback.print_exc()
    finally:
        cur.close()
        close_db_connection(conn)  
#################################################################################################
# Refreshes 52-week high and low statistics for both equity and index symbols.
# For each price table (equity and index):
#   - Collects all symbols that have daily price data.
#   - Computes the maximum high and minimum low from the last year (52 weeks).
#   - Inserts or updates these values into the corresponding 52-week stats table
#     using an UPSERT to keep the record current.
# Commits changes per table and handles errors, ensuring DB cleanup.
#################################################################################################           
def refresh_52week_stats():
    mapping = [
        ("equity_price_data", "equity_52week_stats", "symbol_id"),
        ("index_price_data",  "index_52week_stats",  "index_id")
    ]

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for price_table, stats_table, id_col in mapping:
            log(f"📊 Updating 52W stats for {price_table}")

            # Get all symbol/index ids having daily data
            cur.execute(f"""
                SELECT DISTINCT {id_col}
                FROM {price_table}
                WHERE timeframe='1d'
            """)
            ids = [r[0] for r in cur.fetchall()]
            if not ids:
                log(f"⚠ No daily data found in {price_table}, skipping")
                continue

            # Fetch 52W high/low for all
            placeholders = ",".join("?" * len(ids))
            cur.execute(f"""
                SELECT {id_col}, MAX(high), MIN(low)
                FROM {price_table}
                WHERE timeframe='1d'
                  AND {id_col} IN ({placeholders})
                  AND date >= date('now', '-1 year')
                GROUP BY {id_col}
            """, ids)

            results = [(sid, high, low) for sid, high, low in cur.fetchall() if high is not None]
            if not results:
                log(f"⚠ No 52W data found in {price_table}")
                continue

            # UPSERT
            for sid, high52, low52 in results:
                cur.execute(f"""
                    INSERT INTO {stats_table}
                        ({id_col}, week52_high, week52_low, as_of_date)
                    VALUES (?, ?, ?, date('now'))
                    ON CONFLICT({id_col}) DO UPDATE SET
                        week52_high = excluded.week52_high,
                        week52_low  = excluded.week52_low,
                        as_of_date  = excluded.as_of_date
                """, (sid, high52, low52))

            conn.commit()
            log(f"✅ {stats_table}: Updated {len(results)} rows")

    except Exception as e:
        conn.rollback()
        log(f"❌ 52W update failed: {e}")

    finally:
        cur.close()
        close_db_connection(conn)  