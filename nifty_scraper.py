import json
import os
import random
import time
import warnings
from datetime import datetime, timedelta
import pandas as pd
import requests
import urllib3

warnings.filterwarnings("ignore", category=urllib3.exceptions.NotOpenSSLWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# CONFIG - Pointing strictly to the 'data' folder per your tree
PARQUET_FILE = "data/nifty_data.parquet"
VALUATION_FILE = "data/valuation_data.parquet"
END_DATE = datetime.today()

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


session = requests.Session()
session.headers.update({
    "Origin": "https://niftyindices.com",
    "Referer": "https://niftyindices.com/reports/historical-data",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Content-Type": "application/json; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
})

def fetch_data(index_name, from_dt, to_dt, endpoint_type="tri"):
    """Fetch either TRI or Valuation data."""
    suffix = " INDEX" if "INDEX" not in index_name else ""
    url = "https://niftyindices.com/Backpage.aspx/" + \
          ("getTotalReturnIndexString" if endpoint_type == "tri" else "getHistoricalIndexDataReportString")
    
    payload = {"cinfo": f"{{'name':'{index_name}{suffix}','startDate':'{from_dt.strftime('%d-%b-%Y')}','endDate':'{to_dt.strftime('%d-%b-%Y')}','indexName':'{index_name}'}}"}
    try:
        resp = session.post(url, json=payload, timeout=30, verify=False)
        data = json.loads(resp.json()["d"])
        return pd.DataFrame(data) if data else None
    except:
        return None

def update_file(file_path, endpoint_type, sub_indices_dict):
    if not os.path.exists(file_path):
        print(f"Skipping {file_path}: File not found.")
        return

    df_existing = pd.read_parquet(file_path)
    df_existing["Date"] = pd.to_datetime(df_existing["Date"])
    last_date = df_existing["Date"].max()
    start_fetch = last_date + timedelta(days=1)

    if start_fetch > END_DATE:
        print(f"{file_path} is up to date.")
        return

    new_frames = []
    for sub_cat, indices in sub_indices_dict.items():
        for name in indices:
            df_new = fetch_data(name, start_fetch, END_DATE, endpoint_type)
            if df_new is not None and not df_new.empty:
                # Clean columns based on type
                if endpoint_type == "tri":
                    val_col = next((c for c in ["TotalReturnsIndex", "NTR_Value", "Total_Returns_Index"] if c in df_new.columns), None)
                    df_new = df_new[["Date", val_col]].rename(columns={val_col: "Total_Returns_Index"})
                
                df_new["Date"] = pd.to_datetime(df_new["Date"], dayfirst=True)
                df_new["Index_Name"] = name
                df_new["Sub_Index"] = sub_cat
                new_frames.append(df_new)
            time.sleep(1)

    if new_frames:
        df_final = pd.concat([df_existing, pd.concat(new_frames)], ignore_index=True)
        df_final.drop_duplicates(subset=["Date", "Index_Name"]).sort_values(["Date", "Index_Name"]).to_parquet(file_path, index=False)
        print(f"Updated {file_path}")

def main():
    if datetime.now().weekday() >= 5: return # Skip weekends
    update_file(PARQUET_FILE, "tri", ALL_INDICES)
    # Note: For valuation, we usually only care about the Broad Market ones
    update_file(VALUATION_FILE, "val", {"Broad Market": ALL_INDICES["Broad Market Indices"]})

if __name__ == "__main__":
    main()