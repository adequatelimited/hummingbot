#!/usr/bin/env python3
"""
Biconomy Open Orders Monitor
Fetches currently open orders (requires API credentials)
"""

import requests
import hmac
import hashlib
import time
from datetime import datetime

BASE_URL = "https://api.biconomy.vip"

def load_credentials():
    """Load API credentials from environment"""
    import os
    api_key = os.getenv('BICONOMY_API_KEY')
    api_secret = os.getenv('BICONOMY_API_SECRET')
    
    if not api_key or not api_secret:
        print("Error: Set BICONOMY_API_KEY and BICONOMY_API_SECRET environment variables")
        return None, None
    
    return api_key, api_secret

def create_signature(api_secret, params):
    """Create HMAC SHA256 signature for Biconomy"""
    sorted_items = sorted(params.items())
    encoded = "&".join(f"{key}={value}" for key, value in sorted_items)
    signing_string = f"{encoded}&secret_key={api_secret}"
    digest = hmac.new(api_secret.encode(), signing_string.encode(), hashlib.sha256).hexdigest()
    return digest.upper()

def get_open_orders(api_key, api_secret, symbol="MCM_USDT"):
    """Fetch open orders from Biconomy"""
    url = f"{BASE_URL}/api/v2/private/order/pending"
    
    timestamp = int(time.time() * 1000)
    params = {
        "api_key": api_key,
        "market": symbol,
        "timestamp": timestamp
    }
    signature = create_signature(api_secret, params)
    params["sign"] = signature
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-SITE-ID': '127'
    }
    
    try:
        response = requests.post(
            url,
            data=params,
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f"Error fetching orders: {e}")
        return None

def display_orders(data):
    """Display open orders"""
    if not data or 'result' not in data:
        print("No order data available")
        print(f"Response: {data}")
        return
    
    orders = data['result'].get('records', [])
    
    print(f"\n{'='*100}")
    print(f"Open Orders for MCM-USDT at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*100}\n")
    
    if not orders:
        print("No open orders")
        print(f"\n{'='*100}\n")
        return
    
    print(f"{'Order ID':<15} {'Side':<6} {'Price':<12} {'Amount':<12} {'Filled':<12} {'Total':<12}")
    print("-" * 100)
    
    total_buy_amount = 0
    total_sell_amount = 0
    
    for order in orders:
        order_id = str(order.get('id', ''))[-12:]  # Last 12 chars
        side = 'BUY' if order.get('side', 2) == 2 else 'SELL'
        price = float(order.get('price', 0))
        amount = float(order.get('amount', 0))
        deal_stock = float(order.get('deal_stock', 0))
        total = price * amount
        
        print(f"{order_id:<15} {side:<6} ${price:<11.4f} {amount:<11.2f} {deal_stock:<11.2f} ${total:<11.2f}")
        
        if side == 'BUY':
            total_buy_amount += total
        else:
            total_sell_amount += amount
    
    print(f"\n{'='*100}")
    print(f"Summary:")
    print(f"Total Buy Orders:  ${total_buy_amount:,.2f} USDT")
    print(f"Total Sell Orders: {total_sell_amount:,.2f} MCM")
    print(f"Number of Orders:  {len(orders)}")
    print(f"{'='*100}\n")

if __name__ == "__main__":
    api_key, api_secret = load_credentials()
    
    if api_key and api_secret:
        print("Fetching open orders from Biconomy...")
        data = get_open_orders(api_key, api_secret)
        display_orders(data)
    else:
        print("\nTo use this script, set environment variables:")
        print("export BICONOMY_API_KEY='your_api_key_here'")
        print("export BICONOMY_API_SECRET='your_api_secret_here'")
