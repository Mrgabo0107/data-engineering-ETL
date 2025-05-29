import pandas as pd
import sqlite3 as sq


df = pd.read_csv('data/CSP1_ETF_Stock_Price_History_29.04.24-29.04.25.csv')

print(df.dtypes)
print(df)


df.rename(columns={'Vol.': 'Volume'}, inplace=True)
df['Date'] = pd.to_datetime(df['Date'])
df['Price'] = df['Price'].str.replace(',', '').astype(float)
df['Open'] = df['Open'].str.replace(',', '').astype(float)
df['High'] = df['High'].str.replace(',', '').astype(float)
df['Low'] = df['Low'].str.replace(',', '').astype(float)
df['Volume'] = (df['Volume'].str.replace('K', '').astype(float) * 1000).astype(int)
df['Change %'] = df['Change %'].str.replace('%', '').astype(float) / 100
df['Date'] = df['Date'].dt.strftime('%d-%m-%Y')

print(df.dtypes)
print(df)

conn = sq.connect('cleansed_etf_data/cleaned_data.db')
df.to_sql('etf_data', conn, if_exists='replace', index=False)
conn.close()
