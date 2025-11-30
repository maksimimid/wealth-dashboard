import pandas as pd
from pycoingecko import CoinGeckoAPI
from datetime import datetime, date
import csv
import re
import os
import argparse
from pathlib import Path
import time
import sys

# --- Configuration ---
DATA_DIRECTORY = Path("assets/historic")
CURRENCY = 'usd'
CSV_DELIMITER = ','
MAX_HISTORICAL_DAYS = 365 # CoinGecko Free API limit

# --- CoinGecko Setup ---
cg = CoinGeckoAPI()
COIN_LIST_CACHE = None

# --- Helper Function for New Downloads ---
def get_coingecko_id_and_symbol(symbol_or_id):
    """
    Fetches the CoinGecko ID and the symbol (ticker) for a given input.
    Returns (coin_id, symbol) or (None, None).
    """
    global COIN_LIST_CACHE
    try:
        if COIN_LIST_CACHE is None:
            COIN_LIST_CACHE = cg.get_coins_list()
        input_lower = symbol_or_id.lower()
        
        for coin in COIN_LIST_CACHE:
            if coin['symbol'].lower() == input_lower or coin['id'].lower() == input_lower or coin['name'].lower() == input_lower:
                return (coin['id'], coin['symbol'].upper())

        return (None, None)
    except Exception as e:
        print(f"Error fetching coin list: {e}")
        return (None, None)

# --- Core Download Function ---
def download_and_save_data(coin_id, short_id, vs_currency, start_date=None):
    """
    Downloads historical PRICE data and saves it to a CSV file.
    Filename format: historic-TICKER-CURRENCY-ID.csv
    """
    if not coin_id or not short_id:
        print("❌ Invalid Coin ID or Symbol. Cannot download data.")
        return

    # Filename Format: historic-TICKER-CURRENCY.csv
    filename = DATA_DIRECTORY / f"historic-{short_id.lower()}-{vs_currency.lower()}.csv"
    
    print(f"Processing data for **{short_id.upper()}** (ID: {coin_id})...")

    # Determine the date range for the API call
    if start_date:
        start_timestamp_sec = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_timestamp_sec = int(time.time())
        
        print(f"   -> Fetching data from {start_date} to today.")
        
        data_func = lambda: cg.get_coin_market_chart_range_by_id(
            id=coin_id, 
            vs_currency=vs_currency, 
            from_timestamp=start_timestamp_sec, 
            to_timestamp=end_timestamp_sec
        )
    else:
        # NEW DOWNLOAD MODE: Enforce 365-day limit
        print(f"   -> Fetching {MAX_HISTORICAL_DAYS} days of data (Free API limit).")
        data_func = lambda: cg.get_coin_market_chart_by_id(
            id=coin_id, 
            vs_currency=vs_currency, 
            days=MAX_HISTORICAL_DAYS
        )

    try:
        market_chart = data_func()
    except Exception as e:
        print(f"   ❌ Error fetching data for {coin_id}: {e}")
        return

    # FIX: Change from 'market_caps' to 'prices'
    price_data = market_chart.get('prices', [])

    if not price_data:
        print(f"   ⚠️ No price data found for {short_id.upper()}.")
        return

    # --- Data Formatting ---
    formatted_data = []
    
    for timestamp_ms, price in price_data:
        date_time_obj = datetime.fromtimestamp(timestamp_ms / 1000)
        date_str = date_time_obj.strftime('%Y-%m-%d')
        formatted_data.append([date_str, price]) # Now appending the price
    
    formatted_data.sort(key=lambda x: x[0])

    # --- Save to CSV ---
    
    # FIX: Update Column header to reflect PRICE
    header_col_2 = f"{short_id.upper()} / Price ({vs_currency.upper()})"
    
    write_mode = 'w' 
    write_header = True
    
    if start_date:
        write_mode = 'a'
        write_header = False 
    
    try:
        with open(filename, write_mode, newline='', encoding='utf-8') as csvfile:
            
            if write_header:
                # Write header manually with triple quotes, now with "Price"
                csvfile.write(f'"""Time""","""{header_col_2}"""\n')
            
            # Write data rows
            for row in formatted_data:
                csvfile.write(f'"""{row[0]}""",{row[1]}\n')

        print(f"   ✅ Data saved/updated in **{filename.name}**.")
        
    except Exception as e:
        print(f"   ❌ Error writing file: {e}")


# --- File Handling Functions (No Change) ---
def get_last_date_from_csv(filepath):
    """Reads the CSV file to find the last recorded date."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if len(lines) > 1:
                last_line = lines[-1].strip()
                match = re.search(r'"""(\d{4}-\d{2}-\d{2})"""', last_line)
                if match:
                    last_date_str = match.group(1)
                    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
                    next_day = last_date + pd.Timedelta(days=1).to_pytimedelta()
                    return next_day.strftime('%Y-%m-%d')
        return None 
    except Exception as e:
        print(f"   ⚠️ Could not read last date from {filepath.name}: {e}")
        return None


def update_existing_files():
    """Checks the directory and updates all existing CSV files."""
    
    if not DATA_DIRECTORY.is_dir():
        print(f"Directory not found: {DATA_DIRECTORY}. Creating it now.")
        DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)
        return

    crypto_files = list(DATA_DIRECTORY.glob(f"historic-*-{CURRENCY.lower()}*.csv"))
    
    if not crypto_files:
        print(f"No existing historic files found in {DATA_DIRECTORY} to update.")
        return

    print(f"\n--- 🔄 Starting Update of {len(crypto_files)} Existing Files ---")
    
    current_pattern = re.compile(rf'historic-([a-zA-Z0-9]+)-{CURRENCY.lower()}\.csv')
    legacy_pattern = re.compile(rf'historic-([a-zA-Z0-9]+)-{CURRENCY.lower()}-([a-zA-Z0-9-]+)\.csv')

    for filepath in crypto_files:
        match_current = current_pattern.match(filepath.name)
        match_legacy = legacy_pattern.match(filepath.name)
        short_id = None

        if match_current:
            short_id = match_current.group(1)
        elif match_legacy:
            short_id = match_legacy.group(1)
            new_name = f"historic-{short_id.lower()}-{CURRENCY.lower()}.csv"
            new_path = filepath.with_name(new_name)
            try:
                filepath.rename(new_path)
                print(f"   ℹ️ Renamed legacy file {filepath.name} -> {new_name}")
                filepath = new_path
            except Exception as e:
                print(f"   ⚠️ Failed to rename {filepath.name}: {e}")
                continue
        else:
            print(f"   ⚠️ Skipping file with unexpected format: {filepath.name}")
            continue

        coin_id, resolved_symbol = get_coingecko_id_and_symbol(short_id)
        if not coin_id:
            print(f"   ⚠️ Could not resolve CoinGecko ID for {short_id.upper()}. Skipping {filepath.name}.")
            continue

        start_date = get_last_date_from_csv(filepath)
        
        if start_date is None:
            print(f"   ⚠️ File {filepath.name} is new or empty. Downloading {MAX_HISTORICAL_DAYS} days history instead.")
            download_and_save_data(coin_id, resolved_symbol or short_id.upper(), CURRENCY, start_date=None)
            continue
        
        download_and_save_data(coin_id, resolved_symbol or short_id.upper(), CURRENCY, start_date=start_date)


def main():
    """Main function to handle command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Download and update historical market cap data for cryptocurrencies."
    )
    
    parser.add_argument(
        '--new', 
        type=str, 
        help=f"Download new crypto data. Will be limited to the past {MAX_HISTORICAL_DAYS} days due to Free API limits."
    )
    
    parser.add_argument(
        '--update', 
        action='store_true', 
        help="Update all existing CSV files in the data directory with the latest data."
    )

    parser.add_argument(
        '--all', 
        action='store_true', 
        help="Update all existing files, then prompt to download a new coin."
    )

    if len(sys.argv) == 1:
        print("No arguments provided. Defaulting to update mode. Use --help for more options.")
        DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)
        update_existing_files()
        return

    args = parser.parse_args()
    
    DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

    if args.all:
        update_existing_files()
        
        coin_symbol = input(f"\nEnter a new crypto symbol to download (limited to {MAX_HISTORICAL_DAYS} days), or press Enter to finish: ").strip()
        if coin_symbol:
            coin_id, short_id = get_coingecko_id_and_symbol(coin_symbol)
            if coin_id and short_id:
                download_and_save_data(coin_id, short_id, CURRENCY)
            else:
                print(f"❌ Could not find a CoinGecko ID for symbol: {coin_symbol}")
    
    elif args.update:
        update_existing_files()

    elif args.new:
        coin_symbol = args.new.strip()
        coin_id, short_id = get_coingecko_id_and_symbol(coin_symbol)
        if coin_id and short_id:
            download_and_save_data(coin_id, short_id, CURRENCY)
        else:
            print(f"❌ Could not find a CoinGecko ID for symbol: {coin_symbol}")

if __name__ == "__main__":
    main()