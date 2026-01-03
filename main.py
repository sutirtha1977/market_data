from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from config.logger import log, clear_log
from config.nse_constants import MAIN_MENU_ITEMS, FREQ_COLORS
from services.symbol_service import (
    refresh_equity,
    refresh_indices
)
from services.equity_service import (
    insert_equity_price_data
)
from services.index_service import (
    insert_index_price_data
)
from services.indicator_service import (
    refresh_52week_stats, refresh_indicators
)
from services.bhavcopy_loader import (
    update_hist_delv_pct_from_bhavcopy,
    update_latest_delv_pct_from_bhavcopy
)
from services.incremental_service import incr_yahoo_bhavcopy_download
from services.scanners.backtest_service import backtest_all_scanners
from services.scanners.scanner_HM import scanner_hilega_milega
from services.scanners.scanner_play import scanner_play_multi_years
from db.create_db import create_stock_database

console = Console()

#################################################################################################
# DISPLAY MENU
#################################################################################################
def display_menu() -> None:
    """
    Display the main menu with Rich tables and colored frequency tags.
    """
    table = Table.grid(padding=(0, 3))
    table.add_column("Press")
    table.add_column("Action", style="white")
    table.add_column("Frequency", justify="center")

    for opt, action, freq, _ in MAIN_MENU_ITEMS:
        row_color = FREQ_COLORS.get(freq, "white")
        freq_text = f"[{row_color}]{freq.upper()}[/{row_color}]" if freq else ""
        press_text = f"👉 [bold {row_color}]{opt}[/bold {row_color}]"
        action_text = f"[bold]{action.upper()}[/bold]"
        table.add_row(press_text, action_text, freq_text, style=row_color)

    panel = Panel(table, title="[bold blue]DATA MANAGER[/bold blue]", border_style="bright_blue")
    console.print(panel)
    console.print("\n[bold green]Enter an option and press [yellow]ENTER[/yellow]:[/bold green] ", end="")

#################################################################################################
# PRINT DATAFRAME WITH RICH
#################################################################################################
def print_df_rich(df, max_rows: int = 20) -> None:
    """
    Display a DataFrame in a Rich table.
    """
    table = Table(show_header=True, header_style="bold magenta")
    for col in df.columns:
        table.add_column(str(col))
    for _, row in df.head(max_rows).iterrows():
        table.add_row(*[str(val) for val in row.values])
    console.print(table)
    if len(df) > max_rows:
        console.print(f"... [bold]{len(df) - max_rows}[/] more rows not shown", style="dim")

#################################################################################################
# MENU ACTIONS
#################################################################################################
def action_create_db() -> None:
    clear_log()
    console.print("[bold green]Database Creation Start....[/bold green]")
    create_stock_database(drop_existing=True)
    console.print("[bold green]Database Creation Finish....[/bold green]")
    console.print("[bold green]Equity Symbols Insert Start....[/bold green]")
    refresh_equity()
    console.print("[bold green]Equity Symbols Insert Finish....[/bold green]")
    console.print("[bold green]Index Symbols Insert Start....[/bold green]")
    refresh_indices()
    console.print("[bold green]Index Symbols Insert Finish....[/bold green]")

def action_update_equity_index_prices() -> None:
    clear_log()
    console.print("[bold green]Equity Price Data Update Start....[/bold green]")
    syms = Prompt.ask("Enter symbols (ALL or comma-separated, e.g., RELIANCE,TCS)").upper()
    insert_equity_price_data(syms)
    console.print("[bold green]Equity Price Data Update Finish....[/bold green]")

    if syms.strip() == "ALL":
        console.print("[bold green]Index Price Data Update Start....[/bold green]")
        insert_index_price_data()
        console.print("[bold green]Index Price Data Update Finish....[/bold green]")

def action_delv_pct_hist() -> None:
    clear_log()
    log("SYMBOL | TIMEFRAME | STATUS\n" + "-" * 40 + "\n")
    update_hist_delv_pct_from_bhavcopy()

def action_delv_pct_latest() -> None:
    clear_log()
    log("SYMBOL | TIMEFRAME | STATUS\n" + "-" * 40 + "\n")
    update_latest_delv_pct_from_bhavcopy()

def action_refresh_52week_stats() -> None:
    console.print("[bold green]Update 52 weeks statistics Start....[/bold green]")
    refresh_52week_stats()
    console.print("[bold green]Update 52 weeks statistics Finish....[/bold green]")

def action_refresh_indicators() -> None:
    console.print("[bold green]Refresh Indicators Start....[/bold green]")
    refresh_indicators()
    console.print("[bold green]Refresh Indicators Finish....[/bold green]")

def action_incr_price_data_update() -> None:
    clear_log()
    console.print("[bold green]Download BhavCopy and Update Equity Price Table Start....[/bold green]")
    syms = Prompt.ask("Enter symbols (ALL or comma-separated, e.g., RELIANCE,TCS)").upper()
    incr_yahoo_bhavcopy_download(syms)
    console.print("[bold green]Download BhavCopy and Update Equity Price Table Finish....[/bold green]")

def action_scanner(scanner_type: str) -> None:
    clear_log()
    log("SYMBOL | TIMEFRAME | STATUS\n" + "-" * 40 + "\n")
    if scanner_type == "HM":
        user_date = Prompt.ask(
            "Enter start date (YYYY-MM-DD) or press Enter for auto-detect",
            default=""       # ensures empty Enter returns ""
        ).strip()
        df = scanner_hilega_milega(user_date)
        print(df.head())
    elif scanner_type == "WIP":
        user_year = Prompt.ask("Enter start year:", default="2026").strip()
        user_lookback = Prompt.ask("Enter lookback count:", default="15").strip()
        try:
            lookback_count = int(user_lookback)
        except ValueError:
            print(f"❌ Invalid lookback input: {user_lookback}")
            lookback_count = 0  # fallback default
        print(f"Start Year: {user_year}, Lookback: {lookback_count}")
        scanner_play_multi_years(user_year,lookback_count)

# def action_backtest() -> None:
#     clear_log()
#     log("SYMBOL | TIMEFRAME | STATUS\n" + "-" * 40 + "\n")
#     # scanner_file = Prompt.ask("Enter Scanner CSV file name (e.g., Scanner_HM_01Jan2026.csv)").strip()
#     # if not scanner_file:
#     #     console.print("[bold red]❌ No file entered. Exiting action.[/bold red]")
#     #     return
#     df = backtest_all_scanners()
#     print(df)

#################################################################################################
# MAIN LOOP
#################################################################################################
def data_manager_user_input() -> None:
    try:
        while True:
            display_menu()
            choice = Prompt.ask("[bold green]👉[/bold green]").strip()

            if choice in ("0", "q", "quit", "exit"):
                console.print("[bold green]Exiting...[/bold green]")
                break

            actions = {
                "1": action_create_db,
                "2": action_update_equity_index_prices,
                "3": action_delv_pct_hist,
                "4": action_delv_pct_latest,
                "5": action_refresh_52week_stats,
                "6": action_refresh_indicators,
                "7": action_incr_price_data_update,
                "8": lambda: action_scanner("HM"),
                "9": lambda: action_scanner("WIP"),
                # "10": action_backtest,
            }

            func = actions.get(choice)
            if func:
                func()
            else:
                console.print("[bold red]❌ Invalid choice![/bold red]")

    except KeyboardInterrupt:
        console.print("\n[bold green]Interrupted by user. Exiting...[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")

#################################################################################################
# ENTRY POINT
#################################################################################################
if __name__ == "__main__":
    data_manager_user_input()