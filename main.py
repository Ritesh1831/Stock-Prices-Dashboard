import yfinance as yf
import pandas as pd
import requests
from datetime import datetime
import os

# === CONFIG ===
symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA']

# === TODAY ===
today = datetime.today().strftime('%Y-%m-%d')
print(f"Fetching data for: {today}")

# === Get Power BI push URL from environment ===
POWERBI_URL = os.environ['POWERBI_URL']

# === Download only today's data ===
data = yf.download(
    symbols,
    start=today,
    end=today,
    group_by='ticker',
    progress=False
)

frames = []
for symbol in symbols:
    df = data[symbol].copy().reset_index()
    df['Symbol'] = symbol

    # Add quarter & quarter_year
    df['Quarter'] = df['Date'].dt.quarter
    df['Quarter_Year'] = 'Q' + df['Quarter'].astype(str) + ' ' + df['Date'].dt.year.astype(str)

    df = df.rename(columns={
        'Date': 'date',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume'
    })

    df = df[['Symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'Quarter', 'Quarter_Year']]
    frames.append(df)

final_df = pd.concat(frames)
final_df = final_df.sort_values(by=['date', 'Symbol']).reset_index(drop=True)

print(final_df.head())

# ✅ Save backup CSV
final_df.to_csv("stock_data_today.csv", index=False)
print("✅ Saved to stock_data_today.csv")

# ✅ Prepare JSON payload (date as YYYY-MM-DD)
payload = []
for _, row in final_df.iterrows():
    payload.append({
        "symbol": row['Symbol'],
        "date": row['date'].strftime('%Y-%m-%d'),
        "open": float(row['open']),
        "high": float(row['high']),
        "low": float(row['low']),
        "close": float(row['close']),
        "volume": int(row['volume']),
        "quarter": int(row['Quarter']),
        "quarter_year": row['Quarter_Year']
    })

print(f"Pushing {len(payload)} rows...")

# ✅ Push in chunks
chunk_size = 1000
for i in range(0, len(payload), chunk_size):
    chunk = payload[i:i + chunk_size]
    r = requests.post(POWERBI_URL, json=chunk)
    if r.status_code == 200:
        print(f"✅ Chunk {i//chunk_size + 1} pushed")
    else:
        print(f"❌ Failed chunk {i//chunk_size + 1}: {r.text}")

print("✅ All done.")
