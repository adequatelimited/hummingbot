#!/usr/bin/env python3
"""
Biconomy Ticker Monitor
Fetches and displays 24h ticker stats for MCM-USDT
"""

import requests
from datetime import datetime

BASE_URL = "https://api.biconomy.vip"

def get_ticker(symbol="MCM_USDT"):
    """Fetch 24h ticker from Biconomy"""
    url = f"{BASE_URL}/api/v1/tickers"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f"Error fetching ticker: {e}")
        return None

def display_ticker(data, symbol="MCM_USDT"):
    """Display ticker in a readable format"""
    if not data or 'ticker' not in data:
        print("No ticker data available")
        print(f"Response: {data}")
        return
    
    # Find MCM_USDT in the ticker list
    ticker = None
    for t in data['ticker']:
        if t.get('symbol') == symbol:
            ticker = t
            break
    
    if not ticker:
        print(f"No ticker data for {symbol}")
        return
    
    print(f"\n{'='*60}")
    print(f"MCM-USDT 24h Ticker at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    print(f"Last Price:        ${float(ticker.get('last', 0)):.4f}")
    print(f"24h High:          ${float(ticker.get('high', 0)):.4f}")
    print(f"24h Low:           ${float(ticker.get('low', 0)):.4f}")
    print(f"24h Volume (MCM):  {float(ticker.get('vol', 0)):,.2f}")
    print(f"24h Change %:      {float(ticker.get('change', 0)):.2f}%")
    print(f"Buy Price:         ${float(ticker.get('buy', 0)):.4f}")
    print(f"Sell Price:        ${float(ticker.get('sell', 0)):.4f}")
    
    print(f"\n{'='*60}\n")

if __name__ == "__main__":
    print("Fetching MCM-USDT ticker from Biconomy...")
    data = get_ticker()
    display_ticker(data)
