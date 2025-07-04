#!/usr/bin/env python3
"""
fetch_raw.py

Step 1: Download Lending Club loan stats CSV zip to data/raw/
Future steps will add FRED macro and bank stock downloads.
"""
import os
import requests

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


if __name__ == '__main__':

    # 1. Run the download function
    download_lending_club()

    # 2. Signal completion
    print('Step 1 complete: Lending Club data is in data/raw/')
