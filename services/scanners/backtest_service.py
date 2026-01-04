import os
import pandas as pd
import traceback
from config.paths import SCANNER_FOLDER
from db.connection import get_db_connection, close_db_connection
from services.scanners.export_service import export_to_csv
from config.logger import log  # Assuming you have a log() function

#################################################################################################
# Backtests all CSV scanner files in SCANNER_FOLDER.
# Buys next day's open, holds for `forward_days`, calculates gain/loss.
# No stop loss is applied.
#################################################################################################
def backtest_all_scanners(forward_days: int = 5):
    all_trades = []
    summary_list = []

    try:
        csv_files = [f for f in os.listdir(SCANNER_FOLDER) if f.endswith(".csv")]
        if not csv_files:
            log("❌ No scanner CSVs found")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        conn = get_db_connection()
        log(f"🔍 Starting backtest on {len(csv_files)} scanner files for {forward_days} forward days...")

        for file_name in csv_files:
            path = os.path.join(SCANNER_FOLDER, file_name)
            try:
                df_csv = pd.read_csv(path)
                if 'date' not in df_csv.columns:
                    log(f"⚠ Skipping {file_name} | 'date' column missing")
                    continue
                df_csv['date'] = pd.to_datetime(df_csv['date'])

                required_cols = ['symbol_id','symbol','date']
                missing = [c for c in required_cols if c not in df_csv.columns]
                if missing:
                    log(f"⚠ Skipping {file_name} | Missing columns: {missing}")
                    continue

                trades = []

                for _, row in df_csv.iterrows():
                    try:
                        symbol_id = row['symbol_id']
                        symbol = row['symbol']
                        signal_date = row['date']

                        # ---------------- NEXT DAY ENTRY ----------------
                        entry_sql = """
                            SELECT date, open
                            FROM equity_price_data
                            WHERE symbol_id = ? AND timeframe='1d' AND date > ?
                            ORDER BY date ASC
                            LIMIT 1
                        """
                        entry_df = pd.read_sql(entry_sql, conn, params=(symbol_id, signal_date.strftime("%Y-%m-%d")))
                        if entry_df.empty:
                            continue

                        entry_date = pd.to_datetime(entry_df.iloc[0]['date'])
                        entry_price = entry_df.iloc[0]['open']

                        # ---------------- FORWARD SCAN ----------------
                        fwd_sql = """
                            SELECT date, close
                            FROM equity_price_data
                            WHERE symbol_id = ? AND timeframe='1d' AND date >= ?
                            ORDER BY date ASC
                            LIMIT ?
                        """
                        fwd_df = pd.read_sql(fwd_sql, conn, params=(symbol_id, entry_date.strftime("%Y-%m-%d"), forward_days))
                        if fwd_df.empty:
                            continue

                        exit_price = fwd_df.iloc[-1]['close']
                        exit_date = pd.to_datetime(fwd_df.iloc[-1]['date'])

                        gain_pct = round((exit_price - entry_price) / entry_price * 100, 2)
                        trades.append({
                            "scanner": file_name.replace(".csv",""),
                            "symbol": symbol,
                            "symbol_id": symbol_id,
                            "signal_date": signal_date,
                            "entry_date": entry_date,
                            "entry_price": entry_price,
                            "exit_date": exit_date,
                            "exit_price": exit_price,
                            "gain_pct": gain_pct,
                            "win": gain_pct > 0,
                            "holding_days": len(fwd_df)
                        })
                        all_trades.append(trades[-1])

                    except Exception as e_trade:
                        log(f"⚠ Error processing trade for {row.get('symbol')} on {row.get('date')} | {e_trade}")
                        traceback.print_exc()

                # ---------------- SUMMARY PER FILE ----------------
                if trades:
                    df_trades = pd.DataFrame(trades)
                    summary_list.append({
                        "scanner": file_name.replace(".csv",""),
                        "total_trades": len(df_trades),
                        "win_rate_%": round(df_trades['win'].mean()*100,2),
                        "avg_gain_%": round(df_trades['gain_pct'].mean(),2),
                        "max_gain_%": round(df_trades['gain_pct'].max(),2),
                        "max_loss_%": round(df_trades['gain_pct'].min(),2)
                    })
                    log(f"✅ Backtested {file_name} | Trades: {len(df_trades)} | Win rate: {round(df_trades['win'].mean()*100,2)}%")

            except Exception as e_file:
                log(f"❌ Error processing {file_name} | {e_file}")
                traceback.print_exc()

    except Exception as e_main:
        log(f"❌ Backtest failed | {e_main}")
        traceback.print_exc()

    finally:
        if 'conn' in locals() and conn:
            close_db_connection(conn)
            log("🔒 Database connection closed")

    # ---------------- CREATE DATAFRAMES ----------------
    trades_df = pd.DataFrame(all_trades)
    summary_df = pd.DataFrame(summary_list)

    # ---------------- PER SYMBOL SUMMARY ----------------
    if not trades_df.empty:
        symbol_summary_df = trades_df.groupby('symbol').agg(
            total_trades=('gain_pct','count'),
            win_rate_pct=('win','mean'),
            avg_gain_pct=('gain_pct','mean'),
            max_gain_pct=('gain_pct','max'),
            max_loss_pct=('gain_pct','min')
        ).reset_index()
        symbol_summary_df['win_rate_pct'] = (symbol_summary_df['win_rate_pct']*100).round(2)
        symbol_summary_df['avg_gain_pct'] = symbol_summary_df['avg_gain_pct'].round(2)
        symbol_summary_df['max_gain_pct'] = symbol_summary_df['max_gain_pct'].round(2)
        symbol_summary_df['max_loss_pct'] = symbol_summary_df['max_loss_pct'].round(2)
    else:
        symbol_summary_df = pd.DataFrame()

    # ---------------- EXPORT 3 SEPARATE CSV FILES ----------------
    try:
        export_to_csv(summary_df, SCANNER_FOLDER, "backtest_summary")
        export_to_csv(trades_df, SCANNER_FOLDER, "backtest_trades")
        export_to_csv(symbol_summary_df, SCANNER_FOLDER, "backtest_symbol_summary")
        log(f"✅ Backtest exported | Summary: {len(summary_df)} | Trades: {len(trades_df)} | Symbol summary: {len(symbol_summary_df)}")
    except Exception as e_export:
        log(f"❌ Failed to export CSVs | {e_export}")
        traceback.print_exc()

    return summary_df, trades_df, symbol_summary_df