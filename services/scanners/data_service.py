import pandas as pd
import traceback
from datetime import datetime, timedelta
from db.connection import get_db_connection, close_db_connection

LOOKBACK_DAYS = 365

def get_base_data(
    lookback_days: int = 365,
    start_date: str | None = None
) -> pd.DataFrame:

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
            SELECT
                d.symbol_id,
                s.symbol,
                d.date,

                p.open,
                p.high,
                p.low,
                p.close,
                p.volume,
                p.adj_close,

                d.pct_price_change,

                d.rsi_3,
                d.rsi_9,
                d.ema_rsi_9_3,
                d.wma_rsi_9_21,

                d.sma_20,
                d.sma_50,
                d.sma_200,
                
                d.macd,
                d.macd_signal,

                d.atr_14,

                d.bb_upper   AS bb_upper_d,
                d.bb_middle  AS bb_middle_d,
                d.bb_lower   AS bb_lower_d

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
            'rsi_3','rsi_9','ema_rsi_9_3','wma_rsi_9_21',
            'sma_20','sma_50','sma_200',
            'atr_14',
            'bb_upper_d','bb_middle_d','bb_lower_d'
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
                bb_upper  AS bb_upper_w,
                bb_middle AS bb_middle_w,
                bb_lower  AS bb_lower_w
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
                bb_upper  AS bb_upper_m,
                bb_middle AS bb_middle_m,
                bb_lower  AS bb_lower_m
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
# FETCH PRICE DATA FOR SINGLE SYMBOL + TIMEFRAME
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