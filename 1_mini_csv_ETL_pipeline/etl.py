import pandas as pd
import sqlite3 as sq
import argparse


def read_and_show_pure_data(path):
    df = pd.read_csv(path, )
    print("\n------------------------------------------------------------------------\n")
    print(f" Pure data: \n {df}")
    print(f" Types of pure data readed:\n {df.dtypes}")
    return df

def process_date_column(df):
    df['Date'] = pd.to_datetime(df['Date'])

def process_price_column(df):
    df['Price'] = df['Price'].str.replace(',', '').astype(float)

def process_open_column(df):
    df['Open'] = df['Open'].str.replace(',', '').astype(float)

def process_high_column(df):
    df['High'] = df['High'].str.replace(',', '').astype(float)

def process_low_column(df):
    df['Low'] = df['Low'].str.replace(',', '').astype(float)

def process_volume_column(df):
    df.rename(columns={'Vol.': 'Volume'}, inplace=True)
    df['Volume'] = (df['Volume'].str.replace('K', '').astype(float) * 1000).astype(int)

def process_change_column(df):
    df['Change %'] = df['Change %'].str.replace('%', '').astype(float) / 100

def calculate_SMA(df):
    df_sorted = df.sort_values(by='Date')
    df_sorted['SMA_5'] = df_sorted['Price'].rolling(window=5).mean()
    df_sorted['SMA_20'] = df_sorted['Price'].rolling(window=20).mean()
    df.loc[df_sorted.index, 'SMA_5'] = df_sorted['SMA_5']
    df.loc[df_sorted.index, 'SMA_20'] = df_sorted['SMA_20']

def calculate_EMA(df):
    df_sorted = df.sort_values(by='Date')
    df_sorted['EMA_5'] = df_sorted['Price'].ewm(span=5, adjust=False).mean()
    df_sorted['EMA_20'] = df_sorted['Price'].ewm(span=20, adjust=False).mean()
    df.loc[df_sorted.index, 'EMA_5'] = df_sorted['EMA_5']
    df.loc[df_sorted.index, 'EMA_20'] = df_sorted['EMA_20']


def print_dataframe_data(df):
    print("\n------------------------------------------------------------------------\n")
    print(f" Final dataFrame: \n {df}")
    print(f" Types of cleaned data: \n {df.dtypes}")




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL pipeline for ETF data')
    parser.add_argument("data_csv_path", type=str, help="Path to the CSV file to read")
    parser.add_argument("database_name", type=str, help="Name of the database to save")
    args = parser.parse_args()
    path_to_csv_file = args.data_csv_path
    path_to_database_file = f"cleaned_data/{args.database_name}.db"
    print("aca: ", path_to_database_file)
    df = read_and_show_pure_data(path_to_csv_file)

    #------------------------------------------------------------------------
    # In these first functions, proper column types and formats are defined,
    # percentage values are converted to decimal form, and the ugly column 
    # names are improved.
    #------------------------------------------------------------------------
    process_date_column(df)
    process_price_column(df)
    process_open_column(df)
    process_high_column(df)
    process_low_column(df)
    process_volume_column(df)
    process_change_column(df)
    
    #------------------------------------------------------------------------
    # The following functions will calculate some valuable and interesting values
    # that will be stored in the database.
    #------------------------------------------------------------------------

    # SMA (Simple Moving Average) of the 5 and 20 last values:
    calculate_SMA(df)

    # EMA (Exponential Moving Average), here the pandas.DataFrame methot Exponential
    # Weighted Methods: ewn(), is used to give more weight to the recent prices
    # and then calculate the mean.
    calculate_EMA(df)
    
    #------------------------------------------------------------------------
    # Now the final pandas dataframe is printed and the information
    # is stocked in a database
    #------------------------------------------------------------------------
    print_dataframe_data(df)
    with sq.connect(path_to_database_file) as conn:
        df.to_sql('etf_data', conn, if_exists='replace', index=False)

