"""
update_international_data.py
=============================
Incremental updater for data/international_data.parquet.

Logic
-----
1. Load existing parquet from data/international_data.parquet.
2. Find the last date present.
3. Download only the missing window (last_date + 1 day → today) via yfinance.
4. Append, deduplicate, save back.

Run locally:  python update_international_data.py
"""

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

# ─────────────────────────────────────────────────────────────
#  PATHS
# ─────────────────────────────────────────────────────────────
DATA_DIR    = Path("data")
PARQUET     = DATA_DIR / "international_data.parquet"
DATA_DIR.mkdir(parents=True, exist_ok=True)

FALLBACK_START = "2006-01-01"

TICKERS = {
    "S&P 500":              "^GSPC",
    "NIFTY 50":             "^NSEI",
    "Nasdaq 100 Futures":   "NQ=F",
    "KOSPI":                "^KS11",
    "Shanghai Composite":   "000001.SS",
    "EEM":                  "EEM",
    "TAIEX":                "^TWII",
    "Bovespa":              "^BVSP",
    "Mexico IPC":           "^MXX",
    "S&P Europe 350":       "^SPEUP",
    "Gold":                 "GC=F",
    "Silver":               "SI=F",
    "Bitcoin":              "BTC-USD",
}

TICKER_TO_NAME = {v: k for k, v in TICKERS.items()}

# ─────────────────────────────────────────────────────────────
#  LOAD EXISTING
# ─────────────────────────────────────────────────────────────
def load_existing() -> pd.DataFrame:
    if PARQUET.exists():
        df = pd.read_parquet(PARQUET)
        df.index = pd.to_datetime(df.index)
        print(f"Loaded existing data: {len(df)} rows, up to {df.index.max().date()}")
        return df
    print("No existing parquet found — full historical download.")
    return pd.DataFrame()

# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
def main():
    today = date.today()

    existing = load_existing()

    if existing.empty:
        start_str = FALLBACK_START
    else:
        last_date = existing.index.max().date()
        if last_date >= today:
            print("Already up to date — nothing to do.")
            sys.exit(0)
        start_str = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")

    # yfinance end is exclusive, pass tomorrow to include today
    end_str = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Downloading {start_str} → {today} …")
    new_data = yf.download(
        tickers=list(TICKERS.values()),
        start=start_str,
        end=end_str,
        auto_adjust=True,
        progress=True,
    )["Close"]

    new_data.rename(columns=TICKER_TO_NAME, inplace=True)
    new_data.index = pd.to_datetime(new_data.index)

    if new_data.empty:
        print("No new data returned (market likely closed). Exiting.")
        sys.exit(0)

    # ── Merge & save ─────────────────────────────────────────
    combined = (
        pd.concat([existing, new_data])
        if not existing.empty else new_data
    )
    combined = (
        combined
        [~combined.index.duplicated(keep="last")]
        .sort_index()
    )

    combined.to_parquet(PARQUET, engine="pyarrow", compression="snappy", index=True)

    print(f"✅ Saved {len(combined)} rows → {PARQUET}")
    print(f"   Date span: {combined.index.min().date()} → {combined.index.max().date()}")
    print(f"   Columns  : {combined.columns.tolist()}")


if __name__ == "__main__":
    main()