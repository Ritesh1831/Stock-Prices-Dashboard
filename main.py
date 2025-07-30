import yfinance as yf
import pandas as pd
import requests
from datetime import datetime
import os
import json

# === CONFIG ===
symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA']

# === TODAY ===
today = datetime.today().strftime('%Y-%m-%d')
print(f"Fetching data for: {today}")

# === Get Power BI push URL from environment ===
POWER_BI_URL = os.environ['POWERBI_URL']

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

    # Add Year, Month, Month_Name, Quarter & Quarter_Year, Quarter_Year_Sort
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['Month_Name'] = df['Date'].dt.strftime('%B')
    df['Quarter'] = df['Date'].dt.quarter
    df['Quarter_Year'] = df['Year'].astype(str) + ' Q' + df['Quarter'].astype(str)
    df['Quarter_Year_Sort'] = (df['Year'].astype(str) + df['Quarter'].astype(str)).astype(int)

    df = df.rename(columns={
        'Date': 'date',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume'
    })

    df = df[['Symbol', 'date', 'open', 'high', 'low', 'close', 'volume',
             'Year', 'Month', 'Month_Name', 'Quarter', 'Quarter_Year', 'Quarter_Year_Sort']]
    frames.append(df)

final_df = pd.concat(frames)
final_df = final_df.sort_values(by=['date', 'Symbol']).reset_index(drop=True)

print(final_df.head())

# ✅ Save backup CSV
final_df.to_csv("stock_data_today.csv", index=False)
print("✅ Saved to stock_data_today.csv")

# ✅ Prepare JSON payload with ALL columns
rows = []
for _, row in final_df.iterrows():
    rows.append({
        "Symbol": row['Symbol'],
        "date": row['date'].strftime('%Y-%m-%d'),
        "open": float(row['open']),
        "high": float(row['high']),
        "low": float(row['low']),
        "close": float(row['close']),
        "volume": int(row['volume']),
        "Year": int(row['Year']),
        "Month": int(row['Month']),
        "Month_Name": row['Month_Name'],
        "Quarter": int(row['Quarter']),
        "Quarter_Year": row['Quarter_Year'],
        "Quarter_Year_Sort": int(row['Quarter_Year_Sort'])
    })

print(f"Pushing {len(rows)} rows...")

# ✅ Push in batches
batch_size = 50
for start in range(0, len(rows), batch_size):
    end = start + batch_size
    batch_rows = rows[start:end]
    json_data = json.dumps({"rows": batch_rows})
    response = requests.post(
        POWER_BI_URL,
        headers={"Content-Type": "application/json"},
        data=json_data
    )
    if response.status_code == 200:
        print(f"✅ Batch {start // batch_size + 1} pushed successfully.")
    else:
        print(f"❌ Error pushing batch {start // batch_size + 1}: {response.status_code} {response.text}")

print("✅ All done.")
