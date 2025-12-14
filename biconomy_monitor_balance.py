#!/usr/bin/env python3
"""
Biconomy Balance Monitor
Fetches account balances (requires API credentials)
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
        print("Example: export BICONOMY_API_KEY='your_key'")
        return None, None
    
    return api_key, api_secret

def create_signature(api_secret, params):
    """Create HMAC SHA256 signature for Biconomy"""
    # Sort parameters
    sorted_items = sorted(params.items())
    encoded = "&".join(f"{key}={value}" for key, value in sorted_items)
    signing_string = f"{encoded}&secret_key={api_secret}"
    digest = hmac.new(api_secret.encode(), signing_string.encode(), hashlib.sha256).hexdigest()
    return digest.upper()

def get_account_balance(api_key, api_secret):
    """Fetch account balance from Biconomy"""
    url = f"{BASE_URL}/api/v2/private/user"
    
    timestamp = int(time.time() * 1000)
    params = {
        "api_key": api_key,
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
        print(f"Error fetching balance: {e}")
        return None

def display_balances(data):
    """Display account balances"""
    if not data or 'datas' not in data:
        print("No balance data available")
        print(f"Response: {data}")
        return
    
    # Biconomy returns 'datas' as dict with coin names as keys
    balances_dict = data['datas']
    
    print(f"\n{'='*70}")
    print(f"Account Balances at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    print(f"{'Asset':<10} {'Available':<20} {'Freeze':<20} {'Total':<20}")
    print("-" * 70)
    
    # Filter to show only non-zero balances
    for asset, balance in balances_dict.items():
        available = float(balance.get('available', 0))
        freeze = float(balance.get('freeze', 0))
        total = available + freeze
        
        if total > 0:
            print(f"{asset:<10} {available:<20.8f} {freeze:<20.8f} {total:<20.8f}")
    
    print(f"\n{'='*70}\n")
    
    # Highlight MCM and USDT
    mcm = balances_dict.get('MCM')
    usdt = balances_dict.get('USDT')
    
    if mcm or usdt:
        print("Key Balances for Market Making:")
        print("-" * 70)
        if mcm:
            mcm_avail = float(mcm.get('available', 0))
            mcm_freeze = float(mcm.get('freeze', 0))
            print(f"MCM:  Available={mcm_avail:,.2f}, Freeze={mcm_freeze:,.2f}, Total={mcm_avail+mcm_freeze:,.2f}")
        if usdt:
            usdt_avail = float(usdt.get('available', 0))
            usdt_freeze = float(usdt.get('freeze', 0))
            print(f"USDT: Available=${usdt_avail:,.2f}, Freeze=${usdt_freeze:,.2f}, Total=${usdt_avail+usdt_freeze:,.2f}")
        print()

if __name__ == "__main__":
    api_key, api_secret = load_credentials()
    
    if api_key and api_secret:
        print("Fetching account balances from Biconomy...")
        data = get_account_balance(api_key, api_secret)
        display_balances(data)
    else:
        print("\nTo use this script, set environment variables:")
        print("export BICONOMY_API_KEY='your_api_key_here'")
        print("export BICONOMY_API_SECRET='your_api_secret_here'")
