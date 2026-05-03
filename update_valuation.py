"""
update_valuation.py
====================
Incremental updater for valuation_data.parquet.

Logic
-----
1. Read the existing parquet to find the most-recent date per index.
2. Fetch P/E, P/B, Div-Yield from NiftyIndices API for dates AFTER that.
3. Merge new rows with existing data (deduplicate on Date + Index_Name).
4. Write back to BOTH canonical locations:
     • data/valuation_data.parquet   (the "source of truth" in /data/)
     • valuation_data.parquet        (repo-root copy, kept for compatibility)

Run locally:  python update_valuation.py
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
REPO_ROOT       = Path(__file__).parent
DATA_DIR        = REPO_ROOT / "data"
PARQUET_DATA    = DATA_DIR / "valuation_data.parquet"      # canonical
PARQUET_ROOT    = REPO_ROOT / "valuation_data.parquet"     # root-level copy
LOG_FILE        = REPO_ROOT / "nifty_pepb.log"
NOT_FOUND_FILE  = REPO_ROOT / "nifty_pepb_not_found.txt"

# Fallback start date when parquet is missing entirely
FALLBACK_START  = datetime(2006, 1, 1)

MAX_RETRIES    = 3
RETRY_WAIT     = (5, 15)     # seconds (scaled by attempt number)
REQUEST_SLEEP  = (1, 3)      # polite pause between indices

MONTHS = ["Jan","Feb","Mar","Apr","May","Jun",
          "Jul","Aug","Sep","Oct","Nov","Dec"]

# ─────────────────────────────────────────────────────────────
#  ALL INDICES  (same list as nifty_pepb.py)
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
    """Format datetime as DD-Mon-YYYY expected by the API."""
    return f"{dt.day:02d}-{MONTHS[dt.month-1]}-{dt.year}"


def load_existing_parquet() -> pd.DataFrame:
    """Load from data/valuation_data.parquet; fall back to root copy."""
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
    """
    Return {index_name: start_datetime} for each index.
    Start = last date in parquet + 1 day (so we don't re-fetch known rows).
    Indices missing from parquet get FALLBACK_START.
    """
    if existing.empty:
        return {}   # signal: use FALLBACK_START for everything
    latest = (
        existing.groupby("Index_Name")["Date"].max()
        + timedelta(days=1)
    )
    return latest.to_dict()


# ─────────────────────────────────────────────────────────────
#  API FETCH
# ─────────────────────────────────────────────────────────────
def fetch_pepb(
    index_name: str,
    from_dt: datetime,
    to_dt: datetime,
    attempt: int = 0,
) -> Optional[pd.DataFrame]:
    """
    Fetch P/E, P/B, Div-Yield for `index_name` between `from_dt` and `to_dt`.
    Tries plain name first, then appends ' INDEX' as fallback.
    Retries up to MAX_RETRIES on transient errors.
    """
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

    col_map = {
        "DATE": "Date", "Date": "Date",
        "pe":   "PE",   "PE":   "PE",
        "pb":   "PB",   "PB":   "PB",
        "divYield": "Div_Yield", "DivYield": "Div_Yield", "div_yield": "Div_Yield",
    }

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
                "https://www.niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString",
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
            df.rename(columns=col_map, inplace=True)

            keep = [c for c in ["Date", "PE", "PB", "Div_Yield"] if c in df.columns]
            if "Date" not in keep or len(keep) < 2:
                continue

            df = df[keep].copy()
            df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
            for col in ["PE", "PB", "Div_Yield"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(
                        df[col].astype(str).str.replace(",", ""), errors="coerce"
                    )
            df.dropna(subset=["Date"], inplace=True)
            df.sort_values("Date", inplace=True)

            log.info("  ✓ %d new rows", len(df))
            return df if not df.empty else None

        except (requests.exceptions.SSLError,
                requests.exceptions.RequestException,
                json.JSONDecodeError, ValueError) as exc:
            log.warning("  Error for '%s': %s", name_val, exc)
            continue

    return fetch_pepb(index_name, from_dt, to_dt, attempt + 1)


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
def main():
    today    = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    total_ix = sum(len(v) for v in ALL_INDICES.values())

    log.info("=" * 65)
    log.info("Valuation Data Incremental Updater")
    log.info("Indices : %d  |  End date: %s", total_ix, fmt(today))
    log.info("=" * 65)

    # ── Load existing data ──────────────────────────────────
    existing      = load_existing_parquet()
    start_by_name = get_start_dates(existing)

    new_frames   = []
    not_found    = []
    skipped      = 0
    idx_counter  = 0

    try:
        for sub_index, indices in ALL_INDICES.items():
            log.info("\n── %s ──", sub_index)

            for index_name in indices:
                idx_counter += 1

                # Determine fetch window for this specific index
                start_dt = start_by_name.get(index_name, FALLBACK_START)

                # Nothing to fetch if we're already up to date
                if start_dt.date() >= today.date():
                    log.info("[%d/%d] %s  →  already up to date, skipping.",
                             idx_counter, total_ix, index_name)
                    skipped += 1
                    continue

                log.info("[%d/%d] %s  →  fetching %s → %s",
                         idx_counter, total_ix, index_name,
                         fmt(start_dt), fmt(today))

                df = fetch_pepb(index_name, start_dt, today)

                if df is not None and not df.empty:
                    df["Index_Name"] = index_name
                    df["Sub_Index"]  = sub_index
                    cols = (["Date", "Index_Name", "Sub_Index"] +
                            [c for c in ["PE", "PB", "Div_Yield"] if c in df.columns])
                    new_frames.append(df[cols])
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

    if not existing.empty:
        combined = pd.concat([existing, new_data], ignore_index=True)
    else:
        combined = new_data

    combined = (
        combined
        .drop_duplicates(subset=["Date", "Index_Name"])
        .sort_values(["Sub_Index", "Index_Name", "Date"])
        .reset_index(drop=True)
    )

    # Ensure output directories exist
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

    # ── Save not-found list ───────────────────────────────────
    if not_found:
        NOT_FOUND_FILE.write_text("\n".join(not_found) + "\n", encoding="utf-8")
        log.info("Not-found indices written to '%s' (%d items).",
                 NOT_FOUND_FILE, len(not_found))


if __name__ == "__main__":
    main()