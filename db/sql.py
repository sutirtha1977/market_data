"""
SQL Templates for inserting/updating technical indicators in the database.
Supports equity and index indicator tables with UPSERT logic.
"""

SQL_INSERT = {
    "equity": """
        INSERT INTO {indicator_table} (
            {col_id}, timeframe, date, is_final,
            sma_20, sma_50, sma_200,
            rsi_3, rsi_9, rsi_14,
            bb_upper, bb_middle, bb_lower,
            atr_14, supertrend, supertrend_dir,
            ema_rsi_9_3, wma_rsi_9_21, pct_price_change,
            macd, macd_signal
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT({col_id}, timeframe, date)
        DO UPDATE SET
            is_final        = excluded.is_final,
            sma_20          = excluded.sma_20,
            sma_50          = excluded.sma_50,
            sma_200         = excluded.sma_200,
            rsi_3           = excluded.rsi_3,
            rsi_9           = excluded.rsi_9,
            rsi_14          = excluded.rsi_14,
            bb_upper        = excluded.bb_upper,
            bb_middle       = excluded.bb_middle,
            bb_lower        = excluded.bb_lower,
            atr_14          = excluded.atr_14,
            supertrend      = excluded.supertrend,
            supertrend_dir  = excluded.supertrend_dir,
            ema_rsi_9_3     = excluded.ema_rsi_9_3,
            wma_rsi_9_21    = excluded.wma_rsi_9_21,
            pct_price_change = excluded.pct_price_change,
            macd            = excluded.macd,
            macd_signal     = excluded.macd_signal
    """,

    "index": """
        INSERT INTO {indicator_table} (
            {col_id}, timeframe, date,
            sma_20, sma_50, sma_200,
            rsi_3, rsi_9, rsi_14,
            bb_upper, bb_middle, bb_lower,
            atr_14, supertrend, supertrend_dir,
            ema_rsi_9_3, wma_rsi_9_21, pct_price_change,
            macd, macd_signal
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT({col_id}, timeframe, date)
        DO UPDATE SET
            sma_20          = excluded.sma_20,
            sma_50          = excluded.sma_50,
            sma_200         = excluded.sma_200,
            rsi_3           = excluded.rsi_3,
            rsi_9           = excluded.rsi_9,
            rsi_14          = excluded.rsi_14,
            bb_upper        = excluded.bb_upper,
            bb_middle       = excluded.bb_middle,
            bb_lower        = excluded.bb_lower,
            atr_14          = excluded.atr_14,
            supertrend      = excluded.supertrend,
            supertrend_dir  = excluded.supertrend_dir,
            ema_rsi_9_3     = excluded.ema_rsi_9_3,
            wma_rsi_9_21    = excluded.wma_rsi_9_21,
            pct_price_change = excluded.pct_price_change,
            macd            = excluded.macd,
            macd_signal     = excluded.macd_signal
    """
}