import yfinance as yf
import pandas as pd
from datetime import datetime

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

    df['Quarter'] = 'Q' + df['Date'].dt.quarter.astype(str)
    df['Quarter_Year'] = df['Quarter'] + ' ' + df['Date'].dt.year.astype(str)

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

save_path = "stock_data.csv"
final_df.to_csv(save_path, index=False)
print(f"✅ Saved data locally to: {save_path}")

print(final_df.head())
print(final_df.info())


