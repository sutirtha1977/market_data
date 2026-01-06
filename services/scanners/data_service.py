import pandas as pd
import traceback
from datetime import datetime, timedelta
from db.connection import get_db_connection, close_db_connection
from config.logger import log

LOOKBACK_DAYS = 365

#################################################################################################
# Fetches OHLC price and technical indicators for all symbols over 
# the specified lookback period, merging daily, weekly, and monthly indicator values
#################################################################################################
def get_base_data(lookback_days: int = 365,start_date: str | None = None) -> pd.DataFrame:

    conn = get_db_connection()
    df_daily = pd.DataFrame()

    try:
        # ---------------------------------------------------
        # Date range
        # ---------------------------------------------------
        end_date = (
            datetime.strptime(start_date, "%Y-%m-%d").date()
            if start_date else datetime.now().date()
        )
        start_date = end_date - timedelta(days=lookback_days)

        print("🔍 FETCHING DAILY DATA...")

        # ---------------------------------------------------
        # DAILY data (price + indicators)
        # ---------------------------------------------------
        daily_sql = f"""
            SELECT d.symbol_id, s.symbol, d.date,
                p.open, p.high, p.low, p.close, p.volume, p.adj_close,
                d.pct_price_change, 
                d.rsi_3, d.rsi_9, d.rsi_14, 
                d.ema_rsi_9_3, d.wma_rsi_9_21,
                d.sma_20, d.sma_50, d.sma_200
            FROM equity_indicators d
            JOIN equity_price_data p
              ON p.symbol_id = d.symbol_id
             AND p.date = d.date
             AND p.timeframe = '1d'
            JOIN equity_symbols s
              ON s.symbol_id = d.symbol_id
            WHERE d.timeframe = '1d'
              AND d.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY d.symbol_id, d.date
        """

        df_daily = pd.read_sql(daily_sql, conn)

        if df_daily.empty:
            print("❌ No daily data found")
            return df_daily

        df_daily['date'] = pd.to_datetime(df_daily['date'])

        print(f"📦 DAILY ROWS: {len(df_daily)}")

        # ---------------------------------------------------
        # Numeric conversion
        # ---------------------------------------------------
        numeric_cols = [
            'open','high','low','close','adj_close','volume',
            'pct_price_change',
            'rsi_3','rsi_9','rsi_14','ema_rsi_9_3','wma_rsi_9_21',
            'sma_20','sma_50','sma_200'
        ]

        for col in numeric_cols:
            df_daily[col] = pd.to_numeric(df_daily[col], errors='coerce')

        # ---------------------------------------------------
        # WEEKLY indicators
        # ---------------------------------------------------
        weekly_sql = f"""
            SELECT
                symbol_id,
                date AS weekly_date,
                rsi_3 AS rsi_3_weekly,
                rsi_9 AS rsi_9_weekly,
                rsi_14 AS rsi_14_weekly,
                ema_rsi_9_3 AS ema_rsi_9_3_weekly,
                wma_rsi_9_21 as wma_rsi_9_21_weekly
            FROM equity_indicators
            WHERE timeframe = '1wk'
              AND date BETWEEN '{start_date}' AND '{end_date}'
        """

        df_weekly = pd.read_sql(weekly_sql, conn)
        df_weekly['weekly_date'] = pd.to_datetime(df_weekly['weekly_date'])

        df_daily = df_daily.merge(df_weekly, on='symbol_id', how='left')
        df_daily = df_daily[df_daily['weekly_date'] <= df_daily['date']]
        df_daily = (
            df_daily
            .sort_values(['symbol_id','date','weekly_date'])
            .groupby(['symbol_id','date'], as_index=False)
            .last()
        )

        # ---------------------------------------------------
        # MONTHLY indicators
        # ---------------------------------------------------
        monthly_sql = f"""
            SELECT
                symbol_id,
                date AS monthly_date,
                rsi_3 AS rsi_3_monthly,
                rsi_9 AS rsi_9_monthly,
                rsi_14 AS rsi_14_monthly,
                ema_rsi_9_3 AS ema_rsi_9_3_monthly,
                wma_rsi_9_21 as wma_rsi_9_21_monthly
            FROM equity_indicators
            WHERE timeframe = '1mo'
              AND date BETWEEN '{start_date}' AND '{end_date}'
        """

        df_monthly = pd.read_sql(monthly_sql, conn)
        df_monthly['monthly_date'] = pd.to_datetime(df_monthly['monthly_date'])

        df_daily = df_daily.merge(df_monthly, on='symbol_id', how='left')
        df_daily = df_daily[df_daily['monthly_date'] <= df_daily['date']]
        df_daily = (
            df_daily
            .sort_values(['symbol_id','date','monthly_date'])
            .groupby(['symbol_id','date'], as_index=False)
            .last()
        )

        print(f"✅ FINAL BASE DATA ROWS: {len(df_daily)}")

        return df_daily

    except Exception as e:
        print(f"❌ get_base_data FAILED | {e}")
        traceback.print_exc()
        return df_daily

    finally:
        close_db_connection(conn)
#################################################################################################
#################################################################################################
def get_base_data_weekly(start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:

    conn = get_db_connection()
    df_weekly = pd.DataFrame()

    try:
        
        print("🔍 FETCHING WEEKLY DATA...")
        
        sql = f"""
            WITH weekly_price AS (
                SELECT
                    ep.symbol_id,
                    ep.date,
                    ep.open,
                    ep.high,
                    ep.low,
                    ep.close,
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
                    LAG(wp.close, 1) OVER (
                        PARTITION BY wp.symbol_id 
                        ORDER BY wp.date
                    ) AS close_1w_ago,

                    LAG(wp.sma_20, 2) OVER (
                        PARTITION BY wp.symbol_id 
                        ORDER BY wp.date
                    ) AS sma_20_2w_ago,

                    MIN(wp.low) OVER (
                        PARTITION BY wp.symbol_id 
                        ORDER BY wp.date 
                        ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
                    ) AS min_low_4w
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

                p.open,
                p.high,
                p.low,
                p.close,

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
            ON p.symbol_id = i.symbol_id
            AND p.date = i.date
            JOIN equity_symbols s
            ON p.symbol_id = s.symbol_id

            WHERE p.close > p.sma_20
            AND p.low <= p.min_low_4w
            AND p.sma_20_2w_ago < p.sma_20
            AND p.close >= p.close_1w_ago

            ORDER BY p.symbol_id, p.date;
        """
        df_weekly = pd.read_sql(sql, conn)

        return df_weekly

    except Exception as e:
        print(f"❌ get_base_data FAILED | {e}")
        traceback.print_exc()
        return df_weekly

    finally:
        close_db_connection(conn)
        
#################################################################################################
# Retrieves OHLC price and indicator data for a single symbol and 
# timeframe over a given lookback period.
#################################################################################################
def fetch_price_data_for_symbol_timeframe(conn, symbol_id: int, timeframe: str, lookback_days=LOOKBACK_DAYS):
    """
    Fetch OHLCV + indicators for a given symbol and timeframe.
    """
    from datetime import datetime, timedelta
    import pandas as pd

    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=lookback_days)

    sql = """
        SELECT p.date, p.open, p.high, p.low, p.close, p.volume,
               p.adj_close, d.rsi_3, d.rsi_9, d.ema_rsi_9_3, d.wma_rsi_9_21,
               d.sma_20, d.sma_50, d.sma_200, d.pct_price_change
        FROM equity_price_data p
        LEFT JOIN equity_indicators d
          ON p.symbol_id = d.symbol_id AND p.date = d.date AND d.timeframe = ?
        WHERE p.symbol_id = ? AND p.timeframe = ? AND p.date >= ?
        ORDER BY p.date ASC
    """
    df = pd.read_sql(sql, conn, params=(timeframe, symbol_id, timeframe, start_date))
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
    return df
#################################################################################################
# Identifies the candlestick pattern (Doji, Hammer, Shooting Star, 
# Marubozu, Bullish/Bearish) based on OHLC data.
#################################################################################################   
def get_candle_type(open, high, low, close):
    body = abs(close - open)
    upper_shadow = high - max(open, close)
    lower_shadow = min(open, close) - low

    if high == low:
        return "Doji"

    if close > open:
        color = "Bullish"
    elif close < open:
        color = "Bearish"
    else:
        return "Doji"

    if body < 0.1 * (high - low):
        return "Doji"

    if body <= (high - low) * 0.3 and lower_shadow >= 2 * body and upper_shadow <= 0.3 * body:
        return "Hammer" if color == "Bullish" else "Hanging Man"

    if body <= (high - low) * 0.3 and upper_shadow >= 2 * body and lower_shadow <= 0.3 * body:
        return "Inverted Hammer" if color == "Bullish" else "Shooting Star"

    if upper_shadow < 0.05 * body and lower_shadow < 0.05 * body:
        return f"{color} Marubozu"

    return color