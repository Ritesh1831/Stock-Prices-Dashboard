import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta
import math
import os

# === CONFIG ===
symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA']

# === Use yesterday as default ===
today = datetime.today().date()
yesterday = today - timedelta(days=1)

start_date = yesterday.strftime('%Y-%m-%d')
end_date = today.strftime('%Y-%m-%d')

# === Get Power BI push URL from secret ===
POWER_BI_PUSH_URL = os.environ['POWERBI_URL']

print(f"Fetching data for: {start_date}")

# === Download ===
data = yf.download(
    symbols,
    start=start_date,
    end=end_date,
    group_by='ticker',
    progress=False
)

# === Combine ===
frames = []
for symbol in symbols:
    df = data[symbol].copy()
    if df.empty:
        continue  # skip if no data for this ticker
    df = df.reset_index()
    df['Symbol'] = symbol
    df = df.rename(columns={
        'Date': 'date',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume'
    })
    df = df[['Symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
    frames.append(df)

if not frames:
    print("✅ No new market data found. Market may have been closed.")
    exit()

final_df = pd.concat(frames)
final_df = final_df.sort_values(by=['date', 'Symbol']).reset_index(drop=True)

print(final_df.head())

# === Append new data to local CSV log ===
save_path = "stock_data.csv"

# If file does not exist, write header
if not os.path.exists(save_path):
    final_df.to_csv(save_path, index=False)
else:
    final_df.to_csv(save_path, mode='a', header=False, index=False)

print(f"✅ Appended new rows to {save_path}")

# === Prepare payload ===
payload = []
for _, row in final_df.iterrows():
    payload.append({
        "symbol": row['Symbol'],
        "date": row['date'].isoformat(),
        "open": float(row['open']),
        "high": float(row['high']),
        "low": float(row['low']),
        "close": float(row['close']),
        "volume": int(row['volume'])
    })

print(f"Total rows to push: {len(payload)}")

# ✅ Push in chunks of 1,000
chunk_size = 1000
num_chunks = math.ceil(len(payload) / chunk_size)

for i in range(num_chunks):
    chunk = payload[i * chunk_size : (i + 1) * chunk_size]
    response = requests.post(POWER_BI_PUSH_URL, json=chunk)
    if response.status_code == 200:
        print(f"✅ Successfully pushed chunk {i + 1}/{num_chunks} ({len(chunk)} rows)")
    else:
        print(f"❌ Failed to push chunk {i + 1}/{num_chunks}: {response.text}")

print("✅ All new rows pushed to Power BI!")
