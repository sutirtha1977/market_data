import os
import pandas as pd
import traceback
from config.paths import SCANNER_FOLDER
from db.connection import get_db_connection, close_db_connection
from services.scanners.export_import_service import export_to_csv
from config.logger import log

STARTING_CAPITAL = 1_000_000
FORWARD_DAYS = 5  # Hold for 5 trading days

def backtest_all_scanners():
    """
    Weekly scanner backtest:
    - Buy next day's open after signal.
    - Sell after FORWARD_DAYS.
    - Weekly PnL updates capital for next week.
    - Skip all trades in the last week of the year.
    - Final summary shows only ending capital per file.
    """
    all_summaries = []

    try:
        csv_files = [f for f in os.listdir(SCANNER_FOLDER) if f.endswith(".csv")]
        if not csv_files:
            log("❌ No scanner CSVs found")
            return pd.DataFrame()

        conn = get_db_connection()
        log(f"🔍 Starting backtest for {len(csv_files)} scanner files...")

        for file_name in csv_files:
            path = os.path.join(SCANNER_FOLDER, file_name)
            try:
                df_csv = pd.read_csv(path)
                if df_csv.empty:
                    log(f"⚠ Skipping {file_name} | Empty file")
                    continue

                for col in ['symbol_id', 'symbol', 'date']:
                    if col not in df_csv.columns:
                        log(f"⚠ Skipping {file_name} | Missing column: {col}")
                        continue

                df_csv['date'] = pd.to_datetime(df_csv['date'])
                df_csv = df_csv.sort_values('date')
                df_csv['week'] = df_csv['date'].dt.to_period('W-MON').apply(lambda x: x.start_time)

                capital = STARTING_CAPITAL

                # Determine last week of the year to skip
                last_week_start = df_csv['week'].max()

                # Group by week
                for week_start, week_df in df_csv.groupby('week'):
                    if week_start == last_week_start:
                        log(f"🛑 Skipping last week of the year: {week_start.date()}")
                        continue  # Skip all trades in last week

                    trades_to_run = []

                    for _, row in week_df.iterrows():
                        symbol_id = row['symbol_id']
                        signal_date = row['date']

                        # ---- Entry: next day's open ----
                        entry_sql = """
                            SELECT date, open
                            FROM equity_price_data
                            WHERE symbol_id=? AND timeframe='1d' AND date>?
                            ORDER BY date ASC
                            LIMIT 1
                        """
                        entry_df = pd.read_sql(entry_sql, conn, params=(symbol_id, signal_date.strftime("%Y-%m-%d")))
                        if entry_df.empty:
                            log(f"⚠ Skipping trade {symbol_id} on {signal_date.date()} | No next day data")
                            continue

                        entry_price = entry_df.iloc[0]['open']

                        # ---- Exit: after FORWARD_DAYS ----
                        exit_sql = """
                            SELECT date, close
                            FROM equity_price_data
                            WHERE symbol_id=? AND timeframe='1d' AND date>=?
                            ORDER BY date ASC
                            LIMIT ?
                        """
                        exit_df = pd.read_sql(exit_sql, conn, params=(symbol_id, entry_df.iloc[0]['date'], FORWARD_DAYS))
                        if len(exit_df) < FORWARD_DAYS:
                            log(f"⚠ Skipping trade {symbol_id} on {signal_date.date()} | Not enough forward days")
                            continue

                        exit_price = exit_df.iloc[-1]['close']

                        trades_to_run.append({
                            "symbol_id": symbol_id,
                            "entry_price": entry_price,
                            "exit_price": exit_price
                        })

                    if not trades_to_run:
                        log(f"🗓 Week starting {week_start.date()} | No valid trades")
                        continue

                    # ---- Compute weekly PnL ----
                    capital_per_trade = capital / len(trades_to_run)
                    week_pnl = sum(
                        (t['exit_price'] - t['entry_price']) / t['entry_price'] * capital_per_trade
                        for t in trades_to_run
                    )
                    capital += week_pnl
                    log(f"🗓 Week starting {week_start.date()} | Week PnL: {round(week_pnl,2)} | New capital: {round(capital,2)}")

                # ---- Final summary for file ----
                all_summaries.append({
                    "scanner": file_name.replace(".csv",""),
                    "starting_capital": STARTING_CAPITAL,
                    "ending_capital": round(capital,2),
                    "total_return_%": round((capital/STARTING_CAPITAL - 1)*100,2)
                })
                log(f"✅ Backtest completed for {file_name} | Ending capital: {capital:,.2f}")

            except Exception as e_file:
                log(f"❌ Error processing {file_name} | {e_file}")
                traceback.print_exc()

    finally:
        if 'conn' in locals() and conn:
            close_db_connection(conn)
            log("🔒 Database connection closed")

    # ---- Export summary ----
    if all_summaries:
        final_summary = pd.DataFrame(all_summaries)
        export_to_csv(final_summary, SCANNER_FOLDER, "weekly_scanner_summary")
        log(f"🎯 Weekly scanner summary exported | Files: {len(final_summary)}")
        return final_summary
    else:
        log("⚠ No backtest results to export")
        return pd.DataFrame()