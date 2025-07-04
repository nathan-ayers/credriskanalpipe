#!/usr/bin/env python3
"""
fetch_raw.py

Step 1: Download Lending Club loan stats CSV zip to data/raw/
Future steps will add FRED macro and bank stock downloads.
"""
import os
from dotenv import load_dotenv
import requests
import pandas as pd
import yfinance as yf


# load .env into os.environ
load_dotenv()

# Pull FRED key safely
FRED_API_KEY = os.getenv('FRED_API_KEY')
if not FRED_API_KEY:
    raise RuntimeError("Missing FRED_API_KEY in environment")

# instantiate fred client
from fredapi import Fred
fred = Fred(api_key=FRED_API_KEY)

# Define where to save raw data (project/data/raw)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'raw'))


def download_lending_club():
    """
    Download the Lending Club LoanStats ZIP file and save it locally.

    - Uses requests to stream the download in chunks (safer for large files).
    - Checks HTTP status code and raises an error if something goes wrong.
    """
    # URL for Lending Club's LoanStats CSV ZIP (update if needed)
    url = 'https://resources.lendingclub.com/LoanStats3a.csv.zip'
    output_path = os.path.join(BASE_DIR, 'LoanStats3a.csv.zip')

    print(f'Downloading Lending Club data from {url} ...')
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        # Write in chunks to avoid loading the entire file into memory
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f'Success! Saved to {output_path}')
    else:
        # Raise an exception with details if download fails
        raise Exception(f'Failed to download data: HTTP {response.status_code}')
    
def download_bank_prices(symbols: list):
    """
    Fetch daily OHLC (Open, High, Low, Close) for each symbol via yfinance (or another API)
    and save CSVs to data/raw/bank_prices/.
    """

    os.makedirs(os.path.join(BASE_DIR, 'bank_prices'), exist_ok=True)
    for sym in symbols:
        df = yf.download(sym, period='max', auto_adjust=True)
        out = os.path.join(BASE_DIR, 'bank_prices', f'{sym}.csv')
        df.to_csv(out)
        print(f'Wrote {out}')

def download_macro_indicators():
    """
    Hit the FRED API for unemployment, GDP, CPI (use your API key),
    pull JSON or CSV, and save to data/raw/macro.csv.
    """
    df = pd.DataFrame({
        'unemployment': fred.get_series('UNRATE'),
        'gdp': fred.get_series('GDP'),
        'cpi': fred.get_series('CPIAUCSL')
    })
    out = os.path.join(BASE_DIR, 'macro.csv')
    df.to_csv(out)
    print(f'Wrote {out}')



if __name__ == '__main__':

    download_lending_club()
    download_bank_prices(['JPM', 'BAC', 'C', 'WFC', 'GS'])
    download_macro_indicators()

    print('✅ All raw data downloaded to data/raw/')