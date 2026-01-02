import pandas as pd
import numpy as np
import traceback
from config.logger import log
from typing import Tuple, Callable

#################################################################################################
# Decorator to handle errors in indicator calculations
#################################################################################################
def safe_indicator(func: Callable) -> Callable:
    """
    Safely executes indicator functions, logs failures,
    and returns empty Series with preserved index on error.
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            log(f"❌ {func.__name__} FAILED | {e}")
            traceback.print_exc()

            index = args[0].index if args and hasattr(args[0], "index") else pd.Index([])

            if func.__name__ == "calculate_supertrend":
                return (
                    pd.Series(index=index, dtype=float),
                    pd.Series(index=index, dtype=int)
                )

            return pd.Series(index=index, dtype=float)

    return wrapper

#################################################################################################
@safe_indicator
def calculate_rsi_series(close: pd.Series, period: int) -> pd.Series:
    """Calculates RSI using Wilder's EMA method."""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    return rsi.fillna(100).round(2)

#################################################################################################
@safe_indicator
def calculate_bollinger(
    close: pd.Series, period: int = 20, std_mult: int = 2
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Calculates Bollinger Bands (Upper, Middle, Lower)."""
    mid = close.rolling(period).mean()
    std = close.rolling(period).std()

    upper = mid + std_mult * std
    lower = mid - std_mult * std

    return upper.round(2), mid.round(2), lower.round(2)

#################################################################################################
@safe_indicator
def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculates Average True Range (ATR)."""
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()

    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

    return atr.round(2)

#################################################################################################
@safe_indicator
def calculate_macd(close: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """Calculates MACD line and signal line."""
    ema_12 = close.ewm(span=12, adjust=False).mean()
    ema_26 = close.ewm(span=26, adjust=False).mean()

    macd = ema_12 - ema_26
    signal = macd.ewm(span=9, adjust=False).mean()

    return macd.round(2), signal.round(2)

#################################################################################################
@safe_indicator
def calculate_supertrend(
    df: pd.DataFrame, atr_period: int = 10, multiplier: int = 3
) -> Tuple[pd.Series, pd.Series]:
    """Calculates Supertrend and trend direction."""
    atr = calculate_atr(df, atr_period)
    hl2 = (df["high"] + df["low"]) / 2

    basic_ub = hl2 + multiplier * atr
    basic_lb = hl2 - multiplier * atr

    final_ub = basic_ub.copy()
    final_lb = basic_lb.copy()

    for i in range(1, len(df)):
        final_ub.iloc[i] = (
            basic_ub.iloc[i]
            if basic_ub.iloc[i] < final_ub.iloc[i-1]
            or df["close"].iloc[i-1] > final_ub.iloc[i-1]
            else final_ub.iloc[i-1]
        )

        final_lb.iloc[i] = (
            basic_lb.iloc[i]
            if basic_lb.iloc[i] > final_lb.iloc[i-1]
            or df["close"].iloc[i-1] < final_lb.iloc[i-1]
            else final_lb.iloc[i-1]
        )

    supertrend = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(index=df.index, dtype=int)

    supertrend.iloc[0] = final_ub.iloc[0]
    direction.iloc[0] = -1

    for i in range(1, len(df)):
        if df["close"].iloc[i] > supertrend.iloc[i-1]:
            direction.iloc[i] = 1
            supertrend.iloc[i] = final_lb.iloc[i]
        else:
            direction.iloc[i] = -1
            supertrend.iloc[i] = final_ub.iloc[i]

    return supertrend.round(2), direction

#################################################################################################
@safe_indicator
def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """Calculates Exponential Moving Average."""
    return series.ewm(span=period, adjust=False).mean().round(2)

#################################################################################################
@safe_indicator
def calculate_wma(series: pd.Series, period: int) -> pd.Series:
    """Calculates Weighted Moving Average."""
    weights = np.arange(1, period + 1)
    wma = series.rolling(period).apply(
        lambda x: np.dot(x, weights) / weights.sum(), raw=True
    )
    return wma.round(2)