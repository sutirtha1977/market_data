import traceback
import pandas as pd
from .data_service import get_base_data
from .export_service import export_to_csv

def scanner_WIP(start_date: str | None = None) -> pd.DataFrame:
    """Work-in-progress scanner."""
    try:
        lookback_days = 365
        
        df = get_base_data(lookback_days=lookback_days, start_date=start_date)
        
        df_filtered = df[
            (df['adj_close'] >= 100) &
            (df['rsi_9'] / df['ema_rsi_9_3'] > 1.1) &
            (df['ema_rsi_9_3'] / df['wma_rsi_9_21'] > 1.1) &
            (df['prev_rsi_3'] <= 55) &
            (df['rsi_3'] > 55) &
            (df['rsi_3'] < 80) &
            (df['rsi_3_weekly'] > 50) &
            (df['rsi_3_monthly'] > 50) &
            (df['pct_price_change'] <= 5)
        ].sort_values(['date','symbol'], ascending=[False, True])
        export_to_csv(df_filtered, "WIP")
        return df_filtered
    except Exception as e:
        print(f"❌ scanner_WIP failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()