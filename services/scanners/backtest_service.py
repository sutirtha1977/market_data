import os
import pandas as pd
import traceback
from config.logger import log
from config.paths import SCANNER_FOLDER
from db.connection import get_db_connection, close_db_connection

def backtest_scanner(file_name: str):
    """
    Backtest logic:
    - Buy next day OPEN after signal date
    - Stop-loss = min(ATR_14 on buy date, nearest swing low from past)
    - Target = +10%
    - Exit when first condition hits
    - Computes win-rate, avg gain, max gain, max loss, and max drawdown
    - Lists trades hitting stop-loss
    """
    try:
        filepath = os.path.join(SCANNER_FOLDER, file_name)
        df = pd.read_csv(filepath, dtype={'date': 'object'})
        df['date'] = pd.to_datetime(df['date'])

        required_cols = ['symbol_id', 'symbol', 'date']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"CSV must contain columns: {required_cols}")

        conn = get_db_connection()
        trades = []

        for _, row in df.iterrows():
            symbol_id = row['symbol_id']
            symbol = row['symbol']
            signal_date = row['date']

            # ----------------- Buy next trading day -----------------
            price_sql = """
                SELECT date, open, close
                FROM equity_price_data
                WHERE symbol_id = ? AND timeframe='1d' AND date > ?
                ORDER BY date ASC
                LIMIT 1
            """
            buy_df = pd.read_sql(price_sql, conn, params=(symbol_id, signal_date.strftime("%Y-%m-%d")))
            if buy_df.empty or pd.isna(buy_df.iloc[0]['open']):
                continue

            buy_date = buy_df.iloc[0]['date']
            buy_price = buy_df.iloc[0]['open']

            # ----------------- ATR_14 on buy date -----------------
            atr_sql = """
                SELECT atr_14
                FROM equity_indicators
                WHERE symbol_id = ? AND timeframe='1d' AND date = ?
            """
            atr_df = pd.read_sql(atr_sql, conn, params=(symbol_id, buy_date))
            if atr_df.empty or pd.isna(atr_df.iloc[0]['atr_14']):
                continue
            atr = atr_df.iloc[0]['atr_14']

            # ----------------- Swing low in past 10 days -----------------
            swing_sql = """
                SELECT MIN(low) AS swing_low
                FROM equity_price_data
                WHERE symbol_id = ? AND timeframe='1d' AND date < ?
                  AND date >= date(?, '-10 day')
            """
            swing_df = pd.read_sql(swing_sql, conn, params=(symbol_id, buy_date, buy_date))
            swing_low = swing_df.iloc[0]['swing_low'] if not swing_df.empty else None
            if pd.isna(swing_low):
                swing_low = buy_price - atr  # fallback

            # ----------------- Stop-loss & target -----------------
            stop_loss = min(buy_price - atr, swing_low)
            target = buy_price * 1.05

            # ----------------- Forward scan for exit -----------------
            forward_sql = """
                SELECT date, close
                FROM equity_price_data
                WHERE symbol_id = ? AND timeframe='1d' AND date >= ?
                ORDER BY date ASC
            """
            forward_df = pd.read_sql(forward_sql, conn, params=(symbol_id, buy_date))

            exit_price = None
            exit_date = None
            exit_reason = "EOD"

            for _, p in forward_df.iterrows():
                close_price = p['close']

                # ✅ Stop-loss first
                if close_price <= stop_loss:
                    exit_price = close_price
                    exit_date = p['date']
                    exit_reason = "STOP"
                    break

                # ✅ Target next
                if close_price >= target:
                    exit_price = close_price
                    exit_date = p['date']
                    exit_reason = "TARGET"
                    break

            # If neither hit, exit at last close
            if exit_price is None and not forward_df.empty:
                exit_price = forward_df.iloc[-1]['close']
                exit_date = forward_df.iloc[-1]['date']

            if exit_price is None:
                continue

            gain_pct = round((exit_price - buy_price) / buy_price * 100, 2)
            trades.append({
                "symbol": symbol,
                "buy_date": buy_date,
                "buy_price": round(buy_price,2),
                "atr_14": round(atr,2),
                "swing_low": round(swing_low,2),
                "stop_loss_used": round(stop_loss,2),
                "exit_date": exit_date,
                "exit_price": round(exit_price,2),
                "exit_reason": exit_reason,
                "gain_pct": gain_pct,
                "win": gain_pct > 0
            })

        close_db_connection(conn)

        if not trades:
            print("❌ No valid trades found")
            return

        result_df = pd.DataFrame(trades)

        # ----------------- Summary stats -----------------
        win_rate = result_df['win'].mean() * 100
        avg_return = result_df['gain_pct'].mean()
        max_gain = result_df['gain_pct'].max()
        max_loss = result_df['gain_pct'].min()

        # Max drawdown calculation
        cumulative = (1 + result_df['gain_pct']/100).cumprod()
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max * 100
        max_drawdown = drawdown.min()

        print("\n📈 BACKTEST SUMMARY")
        print(f"Total Trades : {len(result_df)}")
        print(f"Win Rate     : {win_rate:.2f}%")
        print(f"Avg Return   : {avg_return:.2f}%")
        print(f"Max Gain     : {max_gain:.2f}%")
        print(f"Max Loss     : {max_loss:.2f}%")
        print(f"Max Drawdown : {max_drawdown:.2f}%")

        stop_df = result_df[result_df['exit_reason']=="STOP"][[
            "symbol","buy_date","exit_date","exit_price","atr_14","swing_low","stop_loss_used","gain_pct"
        ]]
        print(f"\n📌 Trades that hit stop-loss: {len(stop_df)}")
        print(stop_df)

        return result_df, stop_df

    except Exception as e:
        log(f"❌ backtest_scanner failed | {e}")
        traceback.print_exc()


# def backtest_scanner(file_name: str):
#     """Backtest a scanner CSV by buying next day's open and selling 5 days later."""
#     try:
#         filepath = os.path.join(SCANNER_FOLDER, file_name)
#         df = pd.read_csv(filepath, dtype={'date': 'object'})
#         df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
#         required_cols = ['symbol_id', 'symbol', 'date']
#         if not all(col in df.columns for col in required_cols):
#             raise ValueError(f"File must contain columns: {required_cols}")

#         df = df.sort_values(['date', 'symbol']).reset_index(drop=True)
#         conn = get_db_connection()
#         gains = []

#         for _, row in df.iterrows():
#             symbol_id = row['symbol_id']
#             signal_date = row['date']
#             sql = """
#                 SELECT date, open, close
#                 FROM equity_price_data
#                 WHERE symbol_id = ? AND date >= ? AND timeframe='1d'
#                 ORDER BY date ASC
#                 LIMIT 6
#             """
#             df_price = pd.read_sql(sql, conn, params=(symbol_id, signal_date.strftime("%Y-%m-%d")))
#             if len(df_price) < 2:
#                 gains.append(None)
#                 continue
#             buy_price = df_price.iloc[1]['open']
#             sell_price = df_price.iloc[5]['close'] if len(df_price) >= 6 else df_price.iloc[-1]['close']
#             gains.append(round((sell_price - buy_price)/buy_price*100, 2))

#         df['gain_5d_pct'] = gains
#         df['win_5d'] = df['gain_5d_pct'] > 0
#         print(f"📈 Win Rate (5D): {df['win_5d'].mean()*100:.2f}%")
#         print(f"❌ Max Loss (5D): {df['gain_5d_pct'].min():.2f}%")
#         close_db_connection(conn)
#         print("Backtest completed successfully!")

#     except Exception as e:
#         log(f"❌ backtest_scanner failed | {e}")
#         traceback.print_exc()