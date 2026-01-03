# NSE-specific constants

# ---------------- Menu and colors ----------------
FREQ_COLORS = {
    "Run Once": "bold blue",
    "Run Daily": "bold green",
    "Run As Required": "bold yellow",
    "": "white"
}

MAIN_MENU_ITEMS = [
    ("1", "Create Database, Update Equity and Index Symbols", "Run Once", "red"),
    ("2", "Download Equity and Index data (Yahoo) and update equity and index price tables", "Run Once", "blue"),
    ("3", "Update Delivery % till 29-Dec-2025", "Run Once", "yellow"),
    ("4", "Update Delivery % till Latest DB Date", "Run Once", "blue"),
    ("5", "Update 52 weeks stats", "Run Daily", "yellow"),
    ("6", "Update Equity and Index Indicators", "Run Daily", "yellow"),
    ("7", "Download Yahoo data and Bhavcopy incemental", "Run Daily", "yellow"),
    ("8", "SCANNER: Hilega Milega", "Run Daily", "yellow"),
    ("9", "SCANNER: Playground", "Run As Required", "yellow"),
    # ("10", "Backtest SCANNER: File Name", "Run As Required", "yellow"),
    ("0", "Exit", "", "white"),
]
# ---------------- NSE Indices ----------------
# Format: (index_code, index_name, exchange, yahoo_symbol, category)
NSE_INDICES = [
    ("NIFTY50", "NIFTY 50", "NSE", "^NSEI", "Broad"),
    ("NIFTYNEXT50", "NIFTY Next 50", "NSE", "^NSMIDCP", "Broad"),
    ("NIFTY100", "NIFTY 100", "NSE", "^CNX100", "Broad"),
    ("NIFTY200", "NIFTY 200", "NSE", "^CNX200", "Broad"),
    ("NIFTY500", "NIFTY 500", "NSE", "^CRSLDX", "Broad"),
    ("BANKNIFTY", "NIFTY Bank", "NSE", "^NSEBANK", "Sectoral"),
    ("ITNIFTY", "NIFTY IT", "NSE", "^CNXIT", "Sectoral"),
    ("PHARMANIFTY", "NIFTY Pharma", "NSE", "^CNXPHARMA", "Sectoral"),
    ("FMCGNIFTY", "NIFTY FMCG", "NSE", "^CNXFMCG", "Sectoral"),
    ("AUTONIFTY", "NIFTY Auto", "NSE", "^CNXAUTO", "Sectoral"),
    ("METALNIFTY", "NIFTY Metal", "NSE", "^CNXMETAL", "Sectoral"),
    ("REALTYNIFTY", "NIFTY Realty", "NSE", "^CNXREALTY", "Sectoral"),
    ("PSUBANKNIFTY", "NIFTY PSU Bank", "NSE", "^CNXPSUBANK", "Sectoral"),
    ("INDIAVIX", "India VIX", "NSE", "^INDIAVIX", "Volatility"),
]