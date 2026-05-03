"""
NIFTY P/E, P/B & Dividend Yield Historical Data Scraper
========================================================
Endpoint: https://www.niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString
Fetches full history (Jan 2006 → today) in a single request per index.

Install:  pip install requests pandas openpyxl
Run:      python nifty_pepb.py
Output:   nifty_pepb_all.csv
"""

import json
import logging
import os
import random
import time
import warnings
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
import urllib3

warnings.filterwarnings("ignore")
urllib3.disable_warnings()

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════
OUTPUT_FILE     = "nifty_pepb_all.csv"
CHECKPOINT_FILE = "nifty_pepb_checkpoint.csv"
LOG_FILE        = "nifty_pepb.log"

START_DATE      = datetime(2026, 2, 28)
END_DATE        = datetime.today()

MAX_RETRIES     = 1
RETRY_WAIT      = (5, 15)
REQUEST_SLEEP   = (1, 3)
NOT_FOUND_FILE = "nifty_pepb_not_found.txt"

# Same index list as TRI scraper — all equity sub-indices
ALL_INDICES = {
    "Broad Market Indices": [
        "NIFTY 100",
        "NIFTY 200",
        "NIFTY 50",
        "NIFTY 500",
        "NIFTY FPI 150",
        "NIFTY LARGEMID250",
        "NIFTY MICROCAP250",
        "NIFTY MIDCAP 100",
        "NIFTY MIDCAP 150",
        "NIFTY MIDCAP 50",
        "NIFTY MID SELECT",
        "NIFTY MIDSML 400",
        "NIFTY MIDSMALLCAP400 50:50",
        "NIFTY NEXT 50",
        "NIFTY SMLCAP 100",
        "NIFTY SMLCAP 250",
        "NIFTY SMLCAP 50",
        "NIFTY SMALLCAP 500",
        "NIFTY TOTAL MKT",
        "NIFTY500 LMS EQL",
        "NIFTY500 MULTICAP"
    ],
    "Sectoral Indices": [
        "NIFTY AUTO",
        "NIFTY BANK",
        "NIFTY CEMENT",
        "NIFTY CHEMICALS",
        "NIFTY CONSR DURBL",
        "NIFTY FIN SERVICE",
        "NIFTY FINSRV25 50",
        "NIFTY FINSEREXBNK",
        "NIFTY FMCG",
        "NIFTY HEALTHCARE",
        "NIFTY IT",
        "NIFTY MEDIA",
        "NIFTY METAL",
        "NIFTY MS FIN SERV",
        "NIFTY MIDSML HLTH",
        "NIFTY MS IT TELCM",
        "NIFTY OIL AND GAS",
        "NIFTY PHARMA",
        "NIFTY PVT BANK",
        "NIFTY PSU BANK",
        "NIFTY REALTY",
        "NIFTY500 HEALTH"
    ],
    "Strategy Indices": [
       "NIFTY 50 ARBITRAGE",
        "NIFTY ALPHA 50",
        "NIFTY ALPHALOWVOL",
        "NIFTY AQL 30",
        "NIFTY AQLV 30",
        "NIFTY DIV OPPS 50",
        "NIFTY GROWSECT 15",
        "NIFTY HIGHBETA 50",
        "NIFTY LOW VOL 50",
        "NIFTYM150MOMNTM50",
        "NIFTY M150 QLTY50",
        "NIFTYMS400 MQ 100",
        "NIFTY QLTY LV 30",
        "NIFTYSML250MQ 100",
        "NIFTY SML250 Q50",
        "NIFTY TOP 10 EW",
        "NIFTY TOP 15 EW",
        "NIFTY TOP 20 EW",
        "NIFTY TMMQ 50",
        "NIFTY100 ALPHA 30",
        "NIFTY100 EQL WGT",
        "NIFTY100 LOWVOL30",
        "NIFTY100 QUALTY30",
        "NIFTY200 ALPHA 30",
        "NIFTY200MOMENTM30",
        "NIFTY200 QUALITY 30",
        "NIFTY200 VALUE 30",
        "NIFTY50 DIV POINT",
        "NIFTY50 EQL WGT",
        "NIFTY50 PR 1X INV",
        "NIFTY50 PR 2X LEV",
        "NIFTY50 TR 1X INV",
        "NIFTY50 TR 2X LEV",
        "NIFTY50 USD",
        "NIFTY50 VALUE 20",
        "NIFTY500 EW",
        "NIFTY500 FLEXICAP",
        "NIFTY500 LOWVOL50",
        "NIFTY500MOMENTM50",
        "NIFTY MULTI MQ 50",
        "NIFTY500 MQVLV50",
        "NIFTY500 QLTY50",
        "NIFTY500 VALUE 50"
    ],
    "Thematic Indices": [
         "NIFTY CAPITAL MKT",
        "NIFTY COMMODITIES",
        "NIFTY CONGLOMERATE 50",
        "NIFTY COREHOUSING",
        "NIFTY CPSE",
        "NIFTY ENERGY",
        "NIFTY EV",
        "NIFTY HOUSING",
        "NIFTY CONSUMPTION",
        "NIFTY INDIA CORPORATE GROUP INDEX - ADITYA BIRLA GROUP",
        "NIFTY INDIA CORPORATE GROUP INDEX - MAHINDRA GROUP",
        "NIFTY INDIA CORPORATE GROUP INDEX - TATA GROUP",
        "NIFTY TATA 25 CAP",
        "NIFTY IND DEFENCE",
        "NIFTY IND DIGITAL",
        "NIFTY INFRALOG",
        "NIFTY INTERNET",
        "NIFTY INDIA MFG",
        "NIFTY NEW CONSUMP",
        "NIFTY INDIA RAILWAYS PSU",
        "NIFTY CORP MAATR",
        "NIFTY IND TOURISM",
        "NIFTY INFRA",
        "NIFTY IPO",
        "NIFTY MID LIQ 15",
        "NIFTY MS IND CONS",
        "NIFTY MNC",
        "NIFTY MOBILITY",
        "NIFTY NONCYC CONS",
        "NIFTY PSE",
        "NIFTY REITS & INVITS",
        "NIFTY RURAL",
        "NIFTY SERV SECTOR",
        "NIFTY SHARIAH 25",
        "NIFTY SME EMERGE",
        "NIFTY TRANS LOGIS",
        "NIFTY WAVES",
        "NIFTY100 ENH ESG",
        "NIFTY100 ESG",
        "NIFTY100ESGSECLDR",
        "NIFTY100 LIQ 15",
        "NIFTY50 SHARIAH",
        "NIFTY MULTI MFG",
        "NIFTY MULTI INFRA",
        "NIFTY500 SHARIAH"
    ],
}

# ═══════════════════════════════════════════════════════════════

MONTHS = ["Jan","Feb","Mar","Apr","May","Jun",
          "Jul","Aug","Sep","Oct","Nov","Dec"]

def fmt(dt: datetime) -> str:
    return f"{dt.day:02d}-{MONTHS[dt.month-1]}-{dt.year}"

# ── Logging ────────────────────────────────────────────────────
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── Session ────────────────────────────────────────────────────
session = requests.Session()
session.headers.update({
    "Accept":           "application/json, text/javascript, */*; q=0.01",
    "Accept-Language":  "en-US,en;q=0.9",
    "Content-Type":     "application/json; charset=UTF-8",
    "Origin":           "https://niftyindices.com",
    "Referer":          "https://niftyindices.com/reports/historical-data",
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
})


# ═══════════════════════════════════════════════════════════════
#  API CALL
# ═══════════════════════════════════════════════════════════════
def fetch_pepb(index_name: str, from_dt: datetime, to_dt: datetime,
               attempt: int = 0) -> Optional[pd.DataFrame]:
    """
    Fetch P/E, P/B, Dividend Yield for the full date range in one request.
    Falls back to appending ' INDEX' to name if plain name returns empty.
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

    for name_val in name_variants:
        payload = {
            "cinfo": (
                f"{{'name':'{name_val}',"
                f"'startDate':'{fmt(from_dt)}',"
                f"'endDate':'{fmt(to_dt)}',"
                f"'indexName':'{index_name}'}}"
            )
        }
        log.debug("  Trying name='%s' …", name_val)

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
                log.warning("  Unexpected response structure: %s", str(outer)[:200])
                continue

            records = json.loads(outer["d"])
            if not records:
                log.debug("  Empty for name='%s', trying next variant …", name_val)
                continue

            if name_val != index_name:
                log.info("  ✓ Needed INDEX suffix for '%s'", index_name)

            # ── Parse ──────────────────────────────────────────
            df = pd.DataFrame(records)
            log.debug("  Raw columns: %s", df.columns.tolist())

            # Identify columns — API typically returns:
            # "Date", "PE", "PB", "DivYield" (exact names confirmed below)
            # We map flexibly in case of minor name variations
            # Exact API column names: 'DATE', 'pe', 'pb', 'divYield'
            col_map = {
                "DATE":     "Date",
                "Date":     "Date",
                "pe":       "PE",
                "PE":       "PE",
                "pb":       "PB",
                "PB":       "PB",
                "divYield": "Div_Yield",
                "DivYield": "Div_Yield",
                "div_yield":"Div_Yield",
            }

            df.rename(columns=col_map, inplace=True)

            # Keep only mapped columns that exist
            keep = [c for c in ["Date", "PE", "PB", "Div_Yield"] if c in df.columns]
            if "Date" not in keep:
                log.warning("  No Date column found. Columns: %s", df.columns.tolist())
                continue
            if len(keep) < 2:
                log.warning("  Too few usable columns: %s", keep)
                continue

            df = df[keep].copy()
            df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
            for col in ["PE", "PB", "Div_Yield"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(
                        df[col].astype(str).str.replace(",", ""), errors="coerce"
                    )
            df.dropna(subset=["Date"], inplace=True)
            df = df[df["Date"].notna()]
            df.sort_values("Date", inplace=True)

            log.info("  ✓ %d rows | columns: %s", len(df), df.columns.tolist())
            return df if not df.empty else None

        except (requests.exceptions.SSLError,
                requests.exceptions.RequestException,
                json.JSONDecodeError, ValueError) as e:
            log.warning("  Error for name='%s': %s", name_val, e)
            continue

    # All variants exhausted — retry with back-off
    log.info("  No data for '%s' — will retry.", index_name)
    return fetch_pepb(index_name, from_dt, to_dt, attempt + 1)


# ═══════════════════════════════════════════════════════════════
#  CHECKPOINT
# ═══════════════════════════════════════════════════════════════
def load_checkpoint() -> tuple[pd.DataFrame, set]:
    if os.path.exists(CHECKPOINT_FILE):
        log.info("Loading checkpoint from '%s' …", CHECKPOINT_FILE)
        df = pd.read_csv(CHECKPOINT_FILE, parse_dates=["Date"])
        done = set(zip(df["Sub_Index"], df["Index_Name"]))
        log.info("  Resuming: %d rows, %d indices already done.", len(df), len(done))
        return df, done
    return pd.DataFrame(), set()


def save_checkpoint(frames: list):
    if not frames:
        return
    pd.concat(frames, ignore_index=True).drop_duplicates().to_csv(CHECKPOINT_FILE, index=False)
    log.info("  Checkpoint saved.")


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    total_indices = sum(len(v) for v in ALL_INDICES.values())

    log.info("=" * 65)
    log.info("NIFTY P/E · P/B · Div Yield Scraper  (1 request per index)")
    log.info("Indices    : %d across %d sub-categories", total_indices, len(ALL_INDICES))
    log.info("Date range : %s → %s", fmt(START_DATE), fmt(END_DATE))
    log.info("Output     : %s", OUTPUT_FILE)
    log.info("=" * 65)

    checkpoint_df, done_combos = load_checkpoint()
    all_frames = [checkpoint_df] if not checkpoint_df.empty else []

    idx_counter = 0
    not_found = []

    try:
        for sub_index, indices in ALL_INDICES.items():
            log.info("\n%s\n  Sub-Index: %s\n%s", "─"*60, sub_index, "─"*60)

            for index_name in indices:
                idx_counter += 1
                log.info("\n[%d/%d] %s > %s", idx_counter, total_indices, sub_index, index_name)

                if (sub_index, index_name) in done_combos:
                    log.info("  Already done — skipping.")
                    continue

                df = fetch_pepb(index_name, START_DATE, END_DATE)

                if df is not None:
                    df["Index_Name"] = index_name
                    df["Sub_Index"]  = sub_index
                    # Reorder columns neatly
                    cols = ["Date", "Index_Name", "Sub_Index"] + \
                           [c for c in ["PE", "PB", "Div_Yield"] if c in df.columns]
                    df = df[cols].drop_duplicates(subset=["Date"]).sort_values("Date")
                    all_frames.append(df)
                    log.info("  ✓ '%s': %d rows.", index_name, len(df))
                    save_checkpoint(all_frames)
                else:
                    log.warning("  ✗ No data for '%s'.", index_name)
                    not_found.append(f"{sub_index} > {index_name}")

                time.sleep(random.uniform(*REQUEST_SLEEP))

    except KeyboardInterrupt:
        log.info("\nInterrupted — saving progress …")
    finally:
        if not all_frames:
            log.error("No data collected.")
            return

        final = (
            pd.concat(all_frames, ignore_index=True)
            .drop_duplicates(subset=["Date", "Index_Name"])
            .sort_values(["Sub_Index", "Index_Name", "Date"])
            .reset_index(drop=True)
        )

        final.to_csv(OUTPUT_FILE, index=False)

        log.info("\n" + "=" * 65)
        log.info("SAVED %d rows → '%s'", len(final), OUTPUT_FILE)
        log.info("Indices  : %d", final["Index_Name"].nunique())
        log.info("Columns  : %s", final.columns.tolist())
        log.info("Date span: %s → %s",
                 final["Date"].min().strftime("%d %b %Y"),
                 final["Date"].max().strftime("%d %b %Y"))
        log.info("=" * 65)

        # Save not found indices
        if not_found:
            with open(NOT_FOUND_FILE, "w", encoding="utf-8") as f:
                for item in not_found:
                    f.write(item + "\n")
            log.info("Not found indices saved to '%s' (%d items).",
                    NOT_FOUND_FILE, len(not_found))

        # print("\nSample output:")
        # print(final.head(10).to_string(index=False))


if __name__ == "__main__":
    main()