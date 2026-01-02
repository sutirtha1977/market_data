import pandas as pd
import traceback
from datetime import datetime, timedelta
from db.connection import get_db_connection, close_db_connection

def get_base_data(lookback_days: int = 365, start_date: str | None = None) -> pd.DataFrame:
    """Fetches daily equity indicators and aligns weekly & monthly RSI values."""
    conn = get_db_connection()
    try:
        # DAILY
        # Compute date range
        if start_date:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
        else:
            start_date_obj = datetime.now().date()

        end_date_obj = start_date_obj - timedelta(days=lookback_days)

        # Use BETWEEN in SQL for clarity
        daily_sql = f"""
            SELECT d.symbol_id, s.symbol, d.date, p.adj_close,
                d.rsi_3, d.rsi_9, d.ema_rsi_9_3, d.wma_rsi_9_21,
                d.pct_price_change, p.delv_pct, d.sma_20, d.bb_upper
            FROM equity_indicators d
            JOIN equity_price_data p 
            ON p.symbol_id = d.symbol_id AND p.date = d.date AND p.timeframe='1d'
            JOIN equity_symbols s ON s.symbol_id = d.symbol_id
            WHERE d.timeframe = '1d'
            AND d.date BETWEEN '{end_date_obj}' AND '{start_date_obj}'
            ORDER BY d.symbol_id, d.date
        """
        df_daily = pd.read_sql(daily_sql, conn).sort_values(['symbol_id','date'])
        df_daily['delv_pct'] = pd.to_numeric(df_daily['delv_pct'], errors='coerce')
        df_daily['prev_rsi_3'] = df_daily.groupby('symbol_id')['rsi_3'].shift(1)
        # -------------------- WEEKLY --------------------
        df_weekly = pd.read_sql(f"""
            SELECT symbol_id, date, rsi_3, bb_upper
            FROM equity_indicators
            WHERE timeframe='1wk'
            AND date BETWEEN '{end_date_obj}' AND '{start_date_obj}'
            ORDER BY symbol_id, date
        """, conn)

        # -------------------- MONTHLY --------------------
        df_monthly = pd.read_sql(f"""
            SELECT symbol_id, date, rsi_3, bb_upper
            FROM equity_indicators
            WHERE timeframe='1mo'
            AND date BETWEEN '{end_date_obj}' AND '{start_date_obj}'
            ORDER BY symbol_id, date
        """, conn)

        # Align weekly and monthly <= daily
        df_daily = (
            df_daily
            .merge(df_weekly.rename(columns={'rsi_3':'rsi_3_weekly','date':'weekly_date'}), on='symbol_id', how='left')
            .query("weekly_date <= date")
            .sort_values(['symbol_id','date','weekly_date'])
            .groupby(['symbol_id','date']).last().reset_index()
        )

        df_daily = (
            df_daily
            .merge(df_monthly.rename(columns={'rsi_3':'rsi_3_monthly','date':'monthly_date'}), on='symbol_id', how='left')
            .query("monthly_date <= date")
            .sort_values(['symbol_id','date','monthly_date'])
            .groupby(['symbol_id','date']).last().reset_index()
        )

        return df_daily
    
    except Exception as e:
        print(f"❌ get base data failed | {e}")
        traceback.print_exc()
        return df_daily
    finally:
        close_db_connection(conn)
        
# def get_base_data(lookback_days: int = 365) -> pd.DataFrame:
#     """Fetches daily equity indicators and aligns weekly & monthly RSI values."""
#     conn = get_db_connection()
#     try:
#         # DAILY
#         daily_sql = f"""
#             SELECT d.symbol_id, s.symbol, d.date, p.adj_close,
#                 d.rsi_3, d.rsi_9, d.ema_rsi_9_3, d.wma_rsi_9_21, d.pct_price_change, p.delv_pct, d.sma_20
#             FROM equity_indicators d
#             JOIN equity_price_data p 
#             ON p.symbol_id = d.symbol_id AND p.date = d.date AND p.timeframe='1d'
#             JOIN equity_symbols s ON s.symbol_id = d.symbol_id
#             WHERE d.timeframe = '1d' AND d.date >= date('now','-{lookback_days} days')
#         """
#         df_daily = pd.read_sql(daily_sql, conn).sort_values(['symbol_id','date'])
#         df_daily['delv_pct'] = pd.to_numeric(df_daily['delv_pct'], errors='coerce')
#         df_daily['prev_rsi_3'] = df_daily.groupby('symbol_id')['rsi_3'].shift(1)

#         # WEEKLY
#         df_weekly = pd.read_sql(f"""
#             SELECT symbol_id, date, rsi_3
#             FROM equity_indicators
#             WHERE timeframe='1wk' AND date >= date('now','-{lookback_days} days')
#         """, conn)

#         # MONTHLY
#         df_monthly = pd.read_sql(f"""
#             SELECT symbol_id, date, rsi_3
#             FROM equity_indicators
#             WHERE timeframe='1mo' AND date >= date('now','-{lookback_days} days')
#         """, conn)

#         # Align weekly and monthly <= daily
#         df_daily = (
#             df_daily
#             .merge(df_weekly.rename(columns={'rsi_3':'rsi_3_weekly','date':'weekly_date'}), on='symbol_id', how='left')
#             .query("weekly_date <= date")
#             .sort_values(['symbol_id','date','weekly_date'])
#             .groupby(['symbol_id','date']).last().reset_index()
#         )

#         df_daily = (
#             df_daily
#             .merge(df_monthly.rename(columns={'rsi_3':'rsi_3_monthly','date':'monthly_date'}), on='symbol_id', how='left')
#             .query("monthly_date <= date")
#             .sort_values(['symbol_id','date','monthly_date'])
#             .groupby(['symbol_id','date']).last().reset_index()
#         )

#         return df_daily
    
#     except Exception as e:
#         print(f"❌ get base data failed | {e}")
#         traceback.print_exc()
#         return df_daily
#     finally:
#         close_db_connection(conn)
        
