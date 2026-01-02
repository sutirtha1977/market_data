# backtest_service.py

import os
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import Dash, dcc, html, Input, Output
from db.connection import get_db_connection, close_db_connection
from config.paths import SCANNER_FOLDER


def run_scanner_dashboard(file_name: str):
    """Dash app to visualize scanner signals with Price, EMA(5), and ATR(14)."""

    # -----------------------------
    # Load scanner CSV
    # -----------------------------
    scanner_path = os.path.join(SCANNER_FOLDER, file_name)
    if not os.path.exists(scanner_path):
        print(f"❌ File not found: {scanner_path}")
        return

    df = pd.read_csv(scanner_path)
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")

    # -----------------------------
    # Initialize Dash app
    # -----------------------------
    app = Dash(__name__)
    app.title = f"Scanner Dashboard - {file_name}"

    # -----------------------------
    # Layout
    # -----------------------------
    app.layout = html.Div(style={"display": "flex", "height": "100vh"}, children=[

        # Sidebar
        html.Div(
            style={"width": "20%", "padding": "20px", "backgroundColor": "#f4f4f4"},
            children=[
                html.H3("Scanner Dashboard", style={"textAlign": "center"}),
                html.Label("Select Symbol:"),
                dcc.Dropdown(
                    id="symbol-dropdown",
                    options=[{"label": s, "value": s} for s in sorted(df["symbol"].unique())],
                    value=df["symbol"].unique()[0],
                    clearable=False,
                ),
                html.Hr(),
                html.Div(id="info-box", style={"marginTop": "20px"}),
            ],
        ),

        # Chart area
        html.Div(
            style={"width": "80%", "padding": "20px"},
            children=[dcc.Graph(id="price-chart")],
        ),
    ])

    # -----------------------------
    # Callback
    # -----------------------------
    @app.callback(
        Output("price-chart", "figure"),
        Output("info-box", "children"),
        Input("symbol-dropdown", "value"),
    )
    def update_chart(selected_symbol):
        try:
            # Signal date from scanner
            row = df[df["symbol"] == selected_symbol]
            if row.empty:
                return go.Figure(), "No signal date found."

            signal_date = row.iloc[0]["date"]

            # Fetch symbol_id
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "SELECT symbol_id FROM equity_symbols WHERE symbol = ?",
                (selected_symbol,),
            )
            res = cur.fetchone()
            if not res:
                close_db_connection(conn)
                return go.Figure(), f"{selected_symbol} not found in DB."

            symbol_id = res[0]

            # Fetch price data
            price_df = pd.read_sql(
                """
                SELECT date, open, high, low, close, adj_close
                FROM equity_price_data
                WHERE symbol_id = ?
                  AND timeframe = '1d'
                  AND date >= ?
                ORDER BY date ASC
                """,
                conn,
                params=(symbol_id, signal_date.strftime("%Y-%m-%d")),
            )
            close_db_connection(conn)

            if price_df.empty:
                return go.Figure(), f"No price data for {selected_symbol}"

            price_df["date"] = pd.to_datetime(price_df["date"])

            # -----------------------------
            # Indicators
            # -----------------------------
            # EMA(5)
            price_df["ema_5"] = price_df["close"].ewm(span=5, adjust=False).mean()

            # ATR(14)
            high_low = price_df["high"] - price_df["low"]
            high_close = (price_df["high"] - price_df["close"].shift()).abs()
            low_close = (price_df["low"] - price_df["close"].shift()).abs()
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            price_df["atr_14"] = tr.rolling(14).mean()

            # -----------------------------
            # Plot
            # -----------------------------
            fig = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                row_heights=[0.7, 0.3],
                subplot_titles=("Price", "ATR (14)"),
            )

            # Candlestick
            fig.add_trace(
                go.Candlestick(
                    x=price_df["date"],
                    open=price_df["open"],
                    high=price_df["high"],
                    low=price_df["low"],
                    close=price_df["close"],
                    name="Price",
                    increasing_line_color="green",
                    decreasing_line_color="red",
                ),
                row=1,
                col=1,
            )

            # EMA(5) - BLACK
            fig.add_trace(
                go.Scatter(
                    x=price_df["date"],
                    y=price_df["ema_5"],
                    mode="lines",
                    name="EMA 5",
                    line=dict(color="black", width=1.8),
                ),
                row=1,
                col=1,
            )

            # ATR(14)
            fig.add_trace(
                go.Scatter(
                    x=price_df["date"],
                    y=price_df["atr_14"],
                    mode="lines",
                    name="ATR 14",
                    line=dict(color="orange", width=1.5),
                ),
                row=2,
                col=1,
            )

            fig.update_layout(
                title=f"{selected_symbol} | From {signal_date.date()}",
                template="plotly_white",
                hovermode="x unified",
                xaxis=dict(title="Date"),
                yaxis=dict(title="Price"),
                yaxis2=dict(title="ATR"),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1,
                ),
                margin=dict(l=50, r=50, t=60, b=40),
            )

            info = html.Div([
                html.P(f"Symbol: {selected_symbol}"),
                html.P(f"Signal Date: {signal_date.date()}"),
                html.P(f"Bars Loaded: {len(price_df)}"),
            ])

            return fig, info

        except Exception as e:
            return go.Figure(), f"Error: {e}"

    # -----------------------------
    # Run app
    # -----------------------------
    app.run(debug=False, port=8050)


# -----------------------------
# CLI entry
# -----------------------------
if __name__ == "__main__":
    file_name = input(
        "Enter Scanner CSV file name (e.g., Scanner_HM_01Jan2026.csv): "
    ).strip()
    run_scanner_dashboard(file_name)