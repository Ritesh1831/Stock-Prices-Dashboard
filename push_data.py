import yfinance as yf
import pandas as pd
from datetime import datetime
import requests
import json

symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA']
start_date = '2022-07-01'
end_date = datetime.today().strftime('%Y-%m-%d')

data = yf.download(
    symbols,
    start=start_date,
    end=end_date,
    group_by='ticker'
)

frames = []
for symbol in symbols:
    df = data[symbol].copy()
    df = df.reset_index()
    df['Symbol'] = symbol

    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['Month_Name'] = df['Date'].dt.month_name()
    df['Quarter'] = 'Q' + df['Date'].dt.quarter.astype(str)
    df['Quarter_Year'] = df['Year'].astype(str) + ' ' + df['Quarter']
    df['Quarter_Year_Sort'] = df['Year'].astype(str) + df['Date'].dt.quarter.astype(str)
    df['Quarter_Year_Sort'] = df['Quarter_Year_Sort'].astype(int)

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

save_path = "stock_data.csv"
final_df.to_csv(save_path, index=False)
print(f"✅ Saved data locally to: {save_path}")

POWERBI_URL = "MY_POWER_BI_URL"

final_df['date'] = final_df['date'].astype(str)

rows = final_df.to_dict(orient='records')


batch_size = 50
for start in range(0, len(rows), batch_size):
    end = start + batch_size
    batch = final_df.iloc[start:end]
    json_data = json.dumps({ "rows": json.loads(batch.to_json(orient='records', date_format='iso')) })
    response = requests.post(
        POWERBI_URL,
        headers={"Content-Type": "application/json"},
        data=json_data
    )
    if response.status_code == 200:
        print(f"✅ Batch {start // batch_size + 1} pushed successfully")
    else:
        print(f"❌ Error pushing batch {start // batch_size + 1}: {response.status_code} {response.text}")



print(final_df.head())
print(final_df.info())
