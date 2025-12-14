#!/usr/bin/env python3
"""
Biconomy Orderbook Monitor
Fetches and displays the current orderbook for MCM-USDT
"""

import requests
import json
from datetime import datetime

# Biconomy API endpoint
BASE_URL = "https://api.biconomy.vip"

def get_orderbook(symbol="MCM_USDT", limit=20):
    """Fetch orderbook from Biconomy"""
    url = f"{BASE_URL}/api/v1/depth"
    params = {
        "symbol": symbol,
        "size": limit
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f"Error fetching orderbook: {e}")
        return None

def display_orderbook(data):
    """Display orderbook in a readable format"""
    if not data or 'asks' not in data or 'bids' not in data:
        print("No orderbook data available")
        print(f"Response: {data}")
        return
    
    asks = data.get('asks', [])
    bids = data.get('bids', [])
    
    print(f"\n{'='*60}")
    print(f"MCM-USDT Orderbook at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # Display asks (sells) - reverse order to show lowest first
    print("ASKS (Sell Orders):")
    print(f"{'Price':<15} {'Amount (MCM)':<15} {'Total (USDT)':<15}")
    print("-" * 50)
    for ask in reversed(asks[-10:]):  # Show top 10 lowest asks
        price = float(ask[0])
        amount = float(ask[1])
        total = price * amount
        print(f"${price:<14.4f} {amount:<14.2f} ${total:<14.2f}")
    
    # Calculate spread
    if asks and bids:
        lowest_ask = float(asks[0][0])
        highest_bid = float(bids[0][0])
        mid_price = (lowest_ask + highest_bid) / 2
        spread_pct = ((lowest_ask - highest_bid) / mid_price) * 100
        
        print(f"\n{'='*50}")
        print(f"Mid Price: ${mid_price:.4f}")
        print(f"Spread: ${lowest_ask - highest_bid:.4f} ({spread_pct:.2f}%)")
        print(f"{'='*50}\n")
    
    # Display bids (buys)
    print("BIDS (Buy Orders):")
    print(f"{'Price':<15} {'Amount (MCM)':<15} {'Total (USDT)':<15}")
    print("-" * 50)
    for bid in bids[:10]:  # Show top 10 highest bids
        price = float(bid[0])
        amount = float(bid[1])
        total = price * amount
        print(f"${price:<14.4f} {amount:<14.2f} ${total:<14.2f}")
    
    print("\n")

if __name__ == "__main__":
    print("Fetching MCM-USDT orderbook from Biconomy...")
    data = get_orderbook()
    display_orderbook(data)
