"""
update_nifty_data.py
=====================
Incremental updater for nifty_data.parquet (Total Returns Index).

Logic
-----
1. Read existing data/nifty_data.parquet to find the last date per index.
2. Fetch TRI from NiftyIndices API only for dates AFTER that cutoff.
3. Merge, deduplicate on (Date, Index_Name), write back to both locations:
     • data/nifty_data.parquet   (canonical)
     • nifty_data.parquet        (repo-root copy, kept for compatibility)

Run locally:  python update_nifty_data.py
"""

import json
import logging
import os
import random
import sys
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import urllib3

warnings.filterwarnings("ignore")
urllib3.disable_warnings()

# ─────────────────────────────────────────────────────────────
#  PATHS
# ─────────────────────────────────────────────────────────────
REPO_ROOT    = Path(__file__).parent
DATA_DIR     = REPO_ROOT / "data"
PARQUET_DATA = DATA_DIR / "nifty_data.parquet"      # canonical
PARQUET_ROOT = REPO_ROOT / "nifty_data.parquet"     # root-level copy
LOG_FILE     = REPO_ROOT / "nifty_api.log"
NOT_FOUND_FILE = REPO_ROOT / "nifty_tri_not_found.txt"

FALLBACK_START = datetime(2006, 1, 1)

MAX_RETRIES   = 3
RETRY_WAIT    = (5, 15)
REQUEST_SLEEP = (1, 3)

MONTHS = ["Jan","Feb","Mar","Apr","May","Jun",
          "Jul","Aug","Sep","Oct","Nov","Dec"]

# ─────────────────────────────────────────────────────────────
#  ALL INDICES
# ─────────────────────────────────────────────────────────────
ALL_INDICES = {
    "Broad Market Indices": [
        "NIFTY 100","NIFTY 200","NIFTY 50","NIFTY 500",
        "NIFTY FPI 150","NIFTY LARGEMID250","NIFTY MICROCAP250",
        "NIFTY MIDCAP 100","NIFTY MIDCAP 150","NIFTY MIDCAP 50",
        "NIFTY MID SELECT","NIFTY MIDSML 400","NIFTY MIDSMALLCAP400 50:50",
        "NIFTY NEXT 50","NIFTY SMLCAP 100","NIFTY SMLCAP 250",
        "NIFTY SMLCAP 50","NIFTY SMALLCAP 500","NIFTY TOTAL MKT",
        "NIFTY500 LMS EQL","NIFTY500 MULTICAP",
    ],
    "Sectoral Indices": [
        "NIFTY AUTO","NIFTY BANK","NIFTY CEMENT","NIFTY CHEMICALS",
        "NIFTY CONSR DURBL","NIFTY FIN SERVICE","NIFTY FINSRV25 50",
        "NIFTY FINSEREXBNK","NIFTY FMCG","NIFTY HEALTHCARE","NIFTY IT",
        "NIFTY MEDIA","NIFTY METAL","NIFTY MS FIN SERV","NIFTY MIDSML HLTH",
        "NIFTY MS IT TELCM","NIFTY OIL AND GAS","NIFTY PHARMA",
        "NIFTY PVT BANK","NIFTY PSU BANK","NIFTY REALTY","NIFTY500 HEALTH",
    ],
    "Strategy Indices": [
        "NIFTY 50 ARBITRAGE","NIFTY ALPHA 50","NIFTY ALPHALOWVOL",
        "NIFTY AQL 30","NIFTY AQLV 30","NIFTY DIV OPPS 50","NIFTY GROWSECT 15",
        "NIFTY HIGHBETA 50","NIFTY LOW VOL 50","NIFTYM150MOMNTM50",
        "NIFTY M150 QLTY50","NIFTYMS400 MQ 100","NIFTY QLTY LV 30",
        "NIFTYSML250MQ 100","NIFTY SML250 Q50","NIFTY TOP 10 EW",
        "NIFTY TOP 15 EW","NIFTY TOP 20 EW","NIFTY TMMQ 50",
        "NIFTY100 ALPHA 30","NIFTY100 EQL WGT","NIFTY100 LOWVOL30",
        "NIFTY100 QUALTY30","NIFTY200 ALPHA 30","NIFTY200MOMENTM30",
        "NIFTY200 QUALITY 30","NIFTY200 VALUE 30","NIFTY50 DIV POINT",
        "NIFTY50 EQL WGT","NIFTY50 PR 1X INV","NIFTY50 PR 2X LEV",
        "NIFTY50 TR 1X INV","NIFTY50 TR 2X LEV","NIFTY50 USD",
        "NIFTY50 VALUE 20","NIFTY500 EW","NIFTY500 FLEXICAP",
        "NIFTY500 LOWVOL50","NIFTY500MOMENTM50","NIFTY MULTI MQ 50",
        "NIFTY500 MQVLV50","NIFTY500 QLTY50","NIFTY500 VALUE 50",
    ],
    "Thematic Indices": [
        "NIFTY CAPITAL MKT","NIFTY COMMODITIES","NIFTY CONGLOMERATE 50",
        "NIFTY COREHOUSING","NIFTY CPSE","NIFTY ENERGY","NIFTY EV",
        "NIFTY HOUSING","NIFTY CONSUMPTION",
        "NIFTY INDIA CORPORATE GROUP INDEX - ADITYA BIRLA GROUP",
        "NIFTY INDIA CORPORATE GROUP INDEX - MAHINDRA GROUP",
        "NIFTY INDIA CORPORATE GROUP INDEX - TATA GROUP",
        "NIFTY TATA 25 CAP","NIFTY IND DEFENCE","NIFTY IND DIGITAL",
        "NIFTY INFRALOG","NIFTY INTERNET","NIFTY INDIA MFG",
        "NIFTY NEW CONSUMP","NIFTY INDIA RAILWAYS PSU","NIFTY CORP MAATR",
        "NIFTY IND TOURISM","NIFTY INFRA","NIFTY IPO","NIFTY MID LIQ 15",
        "NIFTY MS IND CONS","NIFTY MNC","NIFTY MOBILITY","NIFTY NONCYC CONS",
        "NIFTY PSE","NIFTY REITS & INVITS","NIFTY RURAL","NIFTY SERV SECTOR",
        "NIFTY SHARIAH 25","NIFTY SME EMERGE","NIFTY TRANS LOGIS","NIFTY WAVES",
        "NIFTY100 ENH ESG","NIFTY100 ESG","NIFTY100ESGSECLDR","NIFTY100 LIQ 15",
        "NIFTY50 SHARIAH","NIFTY MULTI MFG","NIFTY MULTI INFRA","NIFTY500 SHARIAH",
    ],
}

NIFTY_INDEX_REVERSE = {
    'NIFTY SMALLCAP 250': 'NIFTY SMLCAP 250',
}

# ─────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  HTTP SESSION
# ─────────────────────────────────────────────────────────────
session = requests.Session()
session.headers.update({
    "Accept":           "application/json, text/javascript, */*; q=0.01",
    "Accept-Language":  "en-US,en;q=0.9",
    "Content-Type":     "application/json; charset=UTF-8",
    "Origin":           "https://niftyindices.com",
    "Referer":          "https://niftyindices.com/reports/historical-data",
    "User-Agent":       (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
})

# ─────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────
def fmt(dt: datetime) -> str:
    return f"{dt.day:02d}-{MONTHS[dt.month-1]}-{dt.year}"


def load_existing_parquet() -> pd.DataFrame:
    for path in (PARQUET_DATA, PARQUET_ROOT):
        if path.exists():
            log.info("Loading existing data from '%s' …", path)
            df = pd.read_parquet(path)
            df["Date"] = pd.to_datetime(df["Date"])
            log.info("  → %d rows, %d unique indices, up to %s",
                     len(df), df["Index_Name"].nunique(),
                     df["Date"].max().strftime("%d %b %Y"))
            return df
    log.warning("No existing parquet found — will do a full historical fetch.")
    return pd.DataFrame()


def get_start_dates(existing: pd.DataFrame) -> dict[str, datetime]:
    """Last recorded date + 1 day per index; missing indices → FALLBACK_START."""
    if existing.empty:
        return {}
    latest = (
        existing.groupby("Index_Name")["Date"].max()
        + timedelta(days=1)
    )
    return latest.to_dict()


# ─────────────────────────────────────────────────────────────
#  API FETCH
# ─────────────────────────────────────────────────────────────
def fetch_tri(
    index_name: str,
    from_dt: datetime,
    to_dt: datetime,
    attempt: int = 0,
) -> Optional[pd.DataFrame]:
    """Fetch Total Returns Index for `index_name` between `from_dt` and `to_dt`."""
    if attempt >= MAX_RETRIES:
        log.error("  Giving up on '%s' after %d attempts.", index_name, MAX_RETRIES)
        return None

    if attempt > 0:
        wait = random.uniform(*RETRY_WAIT) * attempt
        log.info("  Retry %d/%d — sleeping %.1f s …", attempt, MAX_RETRIES, wait)
        time.sleep(wait)

    name_variants = [index_name]
    if not index_name.endswith(" INDEX"):
        name_variants.append(index_name + " INDEX")

    for name_val in name_variants:
        payload = {
            "cinfo": (
                f"{{'name':'{name_val}',"
                f"'startDate':'{fmt(from_dt)}',"
                f"'endDate':'{fmt(to_dt)}',"
                f"'indexName':'{index_name}'}}"
            )
        }
        try:
            resp = session.post(
                "https://niftyindices.com/Backpage.aspx/getTotalReturnIndexString",
                json=payload,
                timeout=60,
                verify=False,
            )
            resp.raise_for_status()

            outer = resp.json()
            if "d" not in outer:
                log.warning("  Unexpected structure for '%s': %s", name_val, str(outer)[:120])
                continue

            records = json.loads(outer["d"])
            if not records:
                log.debug("  Empty for name='%s'.", name_val)
                continue

            df = pd.DataFrame(records)
            date_col  = next((c for c in ["Date"]                              if c in df.columns), None)
            value_col = next((c for c in ["TotalReturnsIndex", "NTR_Value"]    if c in df.columns), None)

            if not date_col or not value_col:
                log.warning("  Unexpected columns: %s", df.columns.tolist())
                continue

            df = df[[date_col, value_col]].copy()
            df.rename(columns={date_col: "Date", value_col: "Total_Returns_Index"}, inplace=True)
            df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
            df["Total_Returns_Index"] = pd.to_numeric(
                df["Total_Returns_Index"].astype(str).str.replace(",", ""), errors="coerce"
            )
            df.dropna(inplace=True)
            df = df[df["Total_Returns_Index"] > 0]
            df.sort_values("Date", inplace=True)

            log.info("  ✓ %d new rows", len(df))
            return df if not df.empty else None

        except (requests.exceptions.SSLError,
                requests.exceptions.RequestException,
                json.JSONDecodeError, ValueError) as exc:
            log.warning("  Error for '%s': %s", name_val, exc)
            continue

    return fetch_tri(index_name, from_dt, to_dt, attempt + 1)


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
def main():
    today    = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    total_ix = sum(len(v) for v in ALL_INDICES.values())

    log.info("=" * 65)
    log.info("Nifty TRI Incremental Updater")
    log.info("Indices : %d  |  End date: %s", total_ix, fmt(today))
    log.info("=" * 65)

    existing      = load_existing_parquet()
    start_by_name = get_start_dates(existing)

    new_frames  = []
    not_found   = []
    skipped     = 0
    idx_counter = 0

    try:
        for sub_index, indices in ALL_INDICES.items():
            log.info("\n── %s ──", sub_index)

            for index_name in indices:
                idx_counter += 1
                start_dt = start_by_name.get(index_name, FALLBACK_START)

                if start_dt.date() >= today.date():
                    log.info("[%d/%d] %s  →  already up to date, skipping.",
                             idx_counter, total_ix, index_name)
                    skipped += 1
                    continue

                log.info("[%d/%d] %s  →  fetching %s → %s",
                         idx_counter, total_ix, index_name,
                         fmt(start_dt), fmt(today))

                df = fetch_tri(index_name, start_dt, today)

                if df is not None and not df.empty:
                    df["Index_Name"] = index_name
                    df["Sub_Index"]  = sub_index
                    df = df[["Date", "Index_Name", "Sub_Index", "Total_Returns_Index"]]
                    new_frames.append(df)
                else:
                    log.warning("  ✗ No data returned for '%s'.", index_name)
                    not_found.append(f"{sub_index} > {index_name}")

                time.sleep(random.uniform(*REQUEST_SLEEP))

    except KeyboardInterrupt:
        log.info("Interrupted — saving what we have …")

    # ── Merge & save ─────────────────────────────────────────
    if not new_frames:
        log.info("\nNo new rows fetched (skipped=%d). Parquet unchanged.", skipped)
        return

    new_data = pd.concat(new_frames, ignore_index=True)
    log.info("\nNew rows fetched : %d", len(new_data))

    combined = (
        pd.concat([existing, new_data], ignore_index=True)
        if not existing.empty else new_data
    )
    combined = (
        combined
        .drop_duplicates(subset=["Date", "Index_Name"])
        .sort_values(["Sub_Index", "Index_Name", "Date"])
        .reset_index(drop=True)
    )

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(PARQUET_DATA, index=False)
    combined.to_parquet(PARQUET_ROOT, index=False)

    log.info("=" * 65)
    log.info("SAVED  %d total rows → %s", len(combined), PARQUET_DATA)
    log.info("       %d total rows → %s", len(combined), PARQUET_ROOT)
    log.info("Indices  : %d", combined["Index_Name"].nunique())
    log.info("Date span: %s → %s",
             combined["Date"].min().strftime("%d %b %Y"),
             combined["Date"].max().strftime("%d %b %Y"))
    log.info("=" * 65)

    if not_found:
        NOT_FOUND_FILE.write_text("\n".join(not_found) + "\n", encoding="utf-8")
        log.info("Not-found indices written to '%s' (%d items).",
                 NOT_FOUND_FILE, len(not_found))


if __name__ == "__main__":
    main()