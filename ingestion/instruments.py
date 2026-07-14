"""
NSE/BSE instrument universe used by the producer.

Each instrument carries a Yahoo Finance symbol (used to seed a realistic
starting price when USE_LIVE_FEED=true) plus a sector tag that flows all the
way through to the gold analytics tables.
"""

INSTRUMENTS = [
    # symbol,        yahoo,          exchange, sector
    ("RELIANCE",   "RELIANCE.NS",   "NSE", "Energy"),
    ("TCS",        "TCS.NS",        "NSE", "IT"),
    ("HDFCBANK",   "HDFCBANK.NS",   "NSE", "Banking"),
    ("INFY",       "INFY.NS",       "NSE", "IT"),
    ("ICICIBANK",  "ICICIBANK.NS",  "NSE", "Banking"),
    ("HINDUNILVR", "HINDUNILVR.NS", "NSE", "FMCG"),
    ("SBIN",       "SBIN.NS",       "NSE", "Banking"),
    ("BHARTIARTL", "BHARTIARTL.NS", "NSE", "Telecom"),
    ("ITC",        "ITC.NS",        "NSE", "FMCG"),
    ("KOTAKBANK",  "KOTAKBANK.NS",  "NSE", "Banking"),
    ("LT",         "LT.NS",         "NSE", "Infrastructure"),
    ("AXISBANK",   "AXISBANK.NS",   "NSE", "Banking"),
    ("ASIANPAINT", "ASIANPAINT.NS", "NSE", "Materials"),
    ("MARUTI",     "MARUTI.NS",     "NSE", "Auto"),
    ("SUNPHARMA",  "SUNPHARMA.NS",  "NSE", "Pharma"),
    ("TATAMOTORS", "TATAMOTORS.NS", "NSE", "Auto"),
    ("WIPRO",      "WIPRO.NS",      "NSE", "IT"),
    ("NESTLEIND",  "NESTLEIND.NS",  "NSE", "FMCG"),
    ("ULTRACEMCO", "ULTRACEMCO.NS", "NSE", "Materials"),
    ("TITAN",      "TITAN.NS",      "NSE", "Consumer"),
    # A couple of BSE-listed references
    ("SENSEX",     "^BSESN",        "BSE", "Index"),
    ("NIFTY50",    "^NSEI",         "NSE", "Index"),
]

# Fallback seed prices (approx INR) if the live feed is disabled/unreachable.
FALLBACK_PRICES = {
    "RELIANCE": 2950.0, "TCS": 3850.0, "HDFCBANK": 1650.0, "INFY": 1550.0,
    "ICICIBANK": 1150.0, "HINDUNILVR": 2400.0, "SBIN": 820.0, "BHARTIARTL": 1400.0,
    "ITC": 435.0, "KOTAKBANK": 1750.0, "LT": 3600.0, "AXISBANK": 1150.0,
    "ASIANPAINT": 2900.0, "MARUTI": 12500.0, "SUNPHARMA": 1650.0,
    "TATAMOTORS": 980.0, "WIPRO": 480.0, "NESTLEIND": 2500.0,
    "ULTRACEMCO": 11000.0, "TITAN": 3400.0, "SENSEX": 79000.0, "NIFTY50": 24000.0,
}
