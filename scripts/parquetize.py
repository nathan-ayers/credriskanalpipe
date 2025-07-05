"""
parquetize.py

Step 2: Convert raw CSVs & ZIP into Parquet files under data/staging/

- Reads Lending Club ZIP, extracts and cleans CSV, writes Snappy-compressed Parquet.
- Reads individual bank price CSVs, parses dates and writes Parquet per ticker.
- Reads macro CSV, parses index as dates, writes Parquet for economic series.
"""

import os
import zipfile
import pandas as pd

# Define base directories for raw and staged data
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
RAW = os.path.join(BASE, 'raw')            # Raw downloads: ZIPs and CSVs
STAGING = os.path.join(BASE, 'staging')    # Parquet outputs


def ensure_staging():
    """
    Create the staging directory if it does not exist.
    This is where all Parquet files will be saved.
    """
    os.makedirs(STAGING, exist_ok=True)


def parquetize_loans():
    """
    1. Open the Lending Club ZIP archive in RAW/LoanStats3a.csv.zip.
    2. Find the CSV file inside and read it into a pandas DataFrame.
    3. Drop any columns that are entirely NaN (useless data).
    4. Write the cleaned DataFrame out as a Snappy-compressed Parquet file in STAGING.
    """
    zip_path = os.path.join(RAW, 'LoanStats3a.csv.zip')
    # Ensure the ZIP exists
    if not os.path.isfile(zip_path):
        raise FileNotFoundError(f"Loan ZIP not found at {zip_path}")

    # Extract and load CSV from the ZIP
    with zipfile.ZipFile(zip_path, 'r') as z:
        # Identify the first .csv file inside the archive
        csv_files = [f for f in z.namelist() if f.endswith('.csv')]
        if not csv_files:
            raise ValueError("No CSV file found in LoanStats3a.zip")
        csv_name = csv_files[0]

        # Load CSV into DataFrame without low-memory type guessing
        with z.open(csv_name) as f:
            df = pd.read_csv(f, low_memory=False)

    # Drop empty columns to reduce file size and noise
    null_cols = df.columns[df.isna().all()].tolist()
    if null_cols:
        df = df.drop(columns=null_cols)

    # Write to Parquet for efficient storage and querying
    out_path = os.path.join(STAGING, 'loan_stats.parquet')
    df.to_parquet(out_path, compression='snappy', index=False)
    print(f"Wrote loan Parquet: {out_path}")


def parquetize_banks():
    """
    Convert bank price CSVs to Parquet, handling their multi-row headers:
    - Skip the first 3 rows (metrics header, ticker header, blank date label).
    - Assign explicit column names: Date, Price, Close, High, Low, Open, Volume.
    - Parse 'Date' column as datetime.
    - Output one Parquet file per ticker.
    """
    bank_dir = os.path.join(RAW, 'bank_prices')
    if not os.path.isdir(bank_dir):
        raise FileNotFoundError(f"Missing bank_prices dir: {bank_dir}")
    
    # Explicit column names to match data columns
    cols = ['Date', 'Price', 'Close', 'High', 'Low', 'Open', 'Volume']

    for filename in os.listdir(bank_dir):
        if filename.lower().endswith('.csv'):
            csv_path = os.path.join(bank_dir, filename)
            ticker = os.path.splitext(filename)[0]

            # Skip first 3 rows to bypass header metadata
            df = pd.read_csv(
                csv_path,
                skiprows=3,
                header=None,
                names=cols,
                parse_dates=['Date']
            )

            out_path = os.path.join(STAGING, f'{ticker}.parquet')
            df.to_parquet(out_path, index=False)
            print(f"Wrote bank Parquet for {ticker}: {out_path}")


def parquetize_macro():
    """
    1. Read the macroeconomic CSV (macro.csv) with the first column as date index.
    2. Ensure the DataFrame index is datetime for time-series operations.
    3. Write the resulting DataFrame to a Parquet file in STAGING.
    """
    macro_csv = os.path.join(RAW, 'macro.csv')
    if not os.path.isfile(macro_csv):
        raise FileNotFoundError(f"Macro CSV not found: {macro_csv}")

    # Load macro indicators, using first column as index and parsing dates
    df = pd.read_csv(macro_csv, parse_dates=[0], index_col=0)

    # Write macro data to Parquet
    out_path = os.path.join(STAGING, 'macro.parquet')
    df.to_parquet(out_path, index=True)
    print(f"Wrote macro Parquet: {out_path}")


if __name__ == '__main__':
    # Ensure staging area is ready
    ensure_staging()

    # Convert each data source to Parquet
    parquetize_loans()
    parquetize_banks()
    parquetize_macro()

    print('Step 2 complete: Parquet files are available in data/staging/')
