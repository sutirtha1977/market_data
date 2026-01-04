import pandas as pd
from db.connection import get_db_connection, close_db_connection
from config.paths import CSV_FILE
from config.nse_constants import NSE_INDICES
from config.logger import log
import traceback
from datetime import datetime

#################################################################################################       
# Loads and synchronizes NSE equity symbols from a master CSV into the equity_symbols table,
# safely inserting new records and enriching missing metadata (series, listing date, ISIN).
#################################################################################################
def refresh_equity():
    try:
        conn = get_db_connection()
        df = pd.read_csv(CSV_FILE)

        # ---------- Column detection ----------
        symbol_col = next((c for c in df.columns if c.lower() == 'symbol'), 'Symbol')
        name_col = next((c for c in df.columns if c.lower() in ('stock name', 'name')), 'Stock Name')

        series_candidates = [c for c in df.columns if 'series' in c.lower()]
        series_col = series_candidates[0] if series_candidates else None

        listing_candidates = [
            c for c in df.columns
            if 'list' in c.lower() and 'date' in c.lower()
        ]
        listing_col = listing_candidates[0] if listing_candidates else None

        isin_candidates = [c for c in df.columns if 'isin' in c.lower()]
        isin_col = isin_candidates[0] if isin_candidates else None

        # ---------- Build column list ----------
        cols = [symbol_col, name_col]
        if series_col:
            cols.append(series_col)
        if listing_col:
            cols.append(listing_col)
        if isin_col:
            cols.append(isin_col)

        iterable = (
            df[cols]
            .dropna(subset=[symbol_col, name_col])
            .drop_duplicates()
        )

        records = []
        updates_series = []
        updates_listing = []
        updates_isin = []

        # ---------- Row processing ----------
        for _, row in iterable.iterrows():
            symbol = str(row[symbol_col]).strip().upper()
            name = str(row[name_col]).strip()

            # Series
            series = None
            if series_col:
                raw = row.get(series_col)
                if pd.notna(raw):
                    s = str(raw).strip().upper()
                    if s and s not in ('NA', 'N/A', '-'):
                        series = s

            # Listing date
            listing_date = None
            if listing_col:
                raw = row.get(listing_col)
                if pd.notna(raw):
                    dt = pd.to_datetime(raw, errors='coerce')
                    if pd.notna(dt):
                        listing_date = dt.date().isoformat()

            # ISIN
            isin = None
            if isin_col:
                raw = row.get(isin_col)
                if pd.notna(raw):
                    s = str(raw).strip().upper()
                    if s and s not in ('NA', 'N/A', '-'):
                        isin = s

            records.append(
                (symbol, name, 'NSE', series, listing_date, isin)
            )

            if series:
                updates_series.append((series, symbol))
            if listing_date:
                updates_listing.append((listing_date, symbol))
            if isin:
                updates_isin.append((isin, symbol))

        # ---------- Database write ----------
        if records:
            conn.executemany("""
                INSERT OR IGNORE INTO equity_symbols
                (symbol, name, exchange, series, listing_date, isin)
                VALUES (?, ?, ?, ?, ?, ?)
            """, records)

            if updates_series:
                conn.executemany(
                    "UPDATE equity_symbols SET series = ? "
                    "WHERE symbol = ? AND (series IS NULL OR series = '')",
                    updates_series
                )

            if updates_listing:
                conn.executemany(
                    "UPDATE equity_symbols SET listing_date = ? "
                    "WHERE symbol = ? AND listing_date IS NULL",
                    updates_listing
                )

            if updates_isin:
                conn.executemany(
                    "UPDATE equity_symbols SET isin = ? "
                    "WHERE symbol = ? AND (isin IS NULL OR isin = '')",
                    updates_isin
                )

            conn.commit()

            log(
                f"Inserted {len(records)} symbols | "
                f"Updated series:{len(updates_series)}, "
                f"listing_date:{len(updates_listing)}, "
                f"isin:{len(updates_isin)}"
            )
        else:
            log("No symbol records to insert")

    except Exception as e:
        log(f"Error refreshing stock symbols: {e}")
        traceback.print_exc()
        raise
    finally:
        close_db_connection(conn)
#################################################################################################
# Refreshes the index_symbols table using a predefined NSE index list,
# ensuring new indices are inserted and existing ones are reactivated.
#################################################################################################
def refresh_indices():
    try:
        conn = get_db_connection()
        # Safety check: ensure schema matches expectations
        cols = {row[1] for row in conn.execute("PRAGMA table_info(index_symbols)")}
        required = {
            "index_id",
            "index_code",
            "index_name",
            "exchange",
            "yahoo_symbol",
            "category",
            "is_active"
        }

        if not required.issubset(cols):
            raise RuntimeError(
                f"index_symbols table schema mismatch. Found columns: {cols}"
            )
        # Prepare records
        # NSE_INDICES format:
        # (index_code, index_name, exchange, yahoo_symbol, category)
        records = [
            (code, name, exch, yahoo, category, 1)
            for (code, name, exch, yahoo, category) in NSE_INDICES
        ]

        # Insert new indices
        conn.executemany("""
            INSERT OR IGNORE INTO index_symbols
            (index_code, index_name, exchange, yahoo_symbol, category, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
        """, records)

        # Reactivate indices already present
        conn.executemany("""
            UPDATE index_symbols
            SET is_active = 1
            WHERE index_code = ?
        """, [(r[0],) for r in records])

        conn.commit()
        log(f"Index symbols refreshed: {len(records)} total")
    except Exception as e:
        log(f"Error refreshing index symbols: {e}")
        raise
    finally:
        close_db_connection(conn)
#################################################################################################
# Retrieves equity symbol IDs and codes based on user input (ALL or comma-separated list),
# returning results as a Pandas DataFrame for downstream processing.
#################################################################################################       
def retrieve_equity_symbol(symbol, conn):
    try:
        # --- Normalize input ---
        if not symbol or not symbol.strip():
            log("No symbol provided")
            return pd.DataFrame()

        symbol_clean = symbol.strip().upper()

        # --- Get all symbols ---
        if symbol_clean == "ALL":
            return pd.read_sql(
                "SELECT symbol_id, symbol FROM equity_symbols ORDER BY symbol",
                conn
            )

        # --- Parse comma-separated list ---
        symbols = {s.strip().upper() for s in symbol.split(",") if s.strip()}
        if not symbols:
            log("No valid symbols parsed")
            return pd.DataFrame()

        placeholders = ",".join("?" for _ in symbols)

        query = f"""
            SELECT symbol_id, symbol
            FROM equity_symbols
            WHERE symbol IN ({placeholders})
            ORDER BY symbol
        """
        return pd.read_sql(query, conn, params=list(symbols))

    except Exception as e:
        log(f"RETRIEVE SYMBOL FAILED: {e}")
        return pd.DataFrame()
#################################################################################################
# Fetches the most recent available trading date for a given timeframe
# from the equity_price_data table.
#################################################################################################
def get_latest_equity_date(timeframe="1d"):
    conn = get_db_connection()
    try:
        sql = """
            SELECT MAX(date) AS latest_date
            FROM equity_price_data
            WHERE timeframe = ?
        """

        df = pd.read_sql(sql, conn, params=[timeframe])
        latest = df.iloc[0]["latest_date"]

        if not latest:
            return None

        # Convert SQLite date string (YYYY-MM-DD) → Python date
        return datetime.strptime(latest, "%Y-%m-%d").date()

    except Exception as e:
        log(f"❗ Error fetching latest date: {e}")
        return None

    finally:
        close_db_connection(conn)
#################################################################################################
# Returns the latest trading date where delivery percentage (delv_pct)
# is available for all symbols for the specified timeframe.
#################################################################################################
def get_latest_equity_date_no_delv(timeframe="1d"):
    conn = get_db_connection()
    try:
        sql = """
            SELECT date
            FROM equity_price_data
            WHERE timeframe = ?
            AND date IN (
                SELECT date
                FROM equity_price_data
                WHERE timeframe = ?
                GROUP BY date
                HAVING COUNT(*) = COUNT(delv_pct)
            )
            ORDER BY date DESC
            LIMIT 1;
        """
        # df = pd.read_sql(sql, conn, params=[timeframe])
        df = pd.read_sql(sql, conn, params=[timeframe, timeframe])
        
        latest = df.iloc[0]["date"]

        if not latest:
            return None

        # Convert SQLite date string (YYYY-MM-DD) → Python date
        return datetime.strptime(latest, "%Y-%m-%d").date()

    except Exception as e:
        log(f"❗ Error fetching latest date: {e}")
        return None

    finally:
        close_db_connection(conn)