import yfinance as yf
import pandas as pd
import requests
from datetime import datetime
import math
import os

# === CONFIG ===
symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA']
start_date = '2022-07-01'
end_date = datetime.today().strftime('%Y-%m-%d')

# === Get Power BI push URL from environment variable ===
POWER_BI_PUSH_URL = os.environ['POWERBI_URL']

# === Download ===
data = yf.download(
    symbols,
    start=start_date,
    end=end_date,
    group_by='ticker'
)

# === Combine into tidy DataFrame ===
frames = []
for symbol in symbols:
    df = data[symbol].copy()
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

final_df = pd.concat(frames)

# Sort by date & symbol
final_df = final_df.sort_values(by=['date', 'Symbol']).reset_index(drop=True)

print(final_df.head())

# === Save backup CSV ===
save_path = "stock_data.csv"
final_df.to_csv(save_path, index=False)
print(f"✅ Saved latest data to {save_path}")

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

print("✅ All rows pushed to Power BI!")
