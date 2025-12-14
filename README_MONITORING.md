# Biconomy Market Monitoring Scripts

Simple Python scripts to monitor the MCM-USDT market on Biconomy exchange.

## Public API Scripts (No Authentication Required)

### 1. Orderbook Monitor
```bash
python3 biconomy_monitor_orderbook.py
```
Shows current buy and sell orders, mid-price, and spread.

### 2. Ticker Monitor
```bash
python3 biconomy_monitor_ticker.py
```
Shows 24h statistics: price, volume, high/low, etc.

## Private API Scripts (Require Authentication)

### 3. Balance Monitor
```bash
export BICONOMY_API_KEY='your_api_key_here'
export BICONOMY_API_SECRET='your_api_secret_here'
python3 biconomy_monitor_balance.py
```
Shows your account balances (MCM, USDT, etc.)

### 4. Open Orders Monitor
```bash
export BICONOMY_API_KEY='your_api_key_here'
export BICONOMY_API_SECRET='your_api_secret_here'
python3 biconomy_monitor_orders.py
```
Shows all your currently open orders on MCM-USDT.

## Installation

Install required dependency:
```bash
pip3 install requests
```

## Quick Dashboard

Run all public monitors:
```bash
python3 biconomy_monitor_ticker.py && python3 biconomy_monitor_orderbook.py
```

## Continuous Monitoring

Watch the market every 10 seconds:
```bash
watch -n 10 python3 biconomy_monitor_ticker.py
```

## Troubleshooting

If you get API errors:
1. Check Biconomy API documentation: https://api.biconomy.com
2. Verify API endpoints are correct
3. For private endpoints, ensure credentials are set correctly
4. Check if your IP is whitelisted on Biconomy (if required)
