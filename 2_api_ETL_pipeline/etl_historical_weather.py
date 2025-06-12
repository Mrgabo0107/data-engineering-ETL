import pandas as pd
import datetime
import requests
import sqlite3 as sq
from utils import print_data_f


def extract(latitude, longitude):
    url = "https://archive-api.open-meteo.com/v1/archive"
    today = datetime.date.today()
    start_date = (today - datetime.timedelta(days=365)).isoformat()
    end_date = today.isoformat()

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
        "timezone": "auto"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if "daily" not in data:
            raise ValueError("No 'daily' key found in API response.")
        return pd.DataFrame(data=data["daily"])
    except requests.exceptions.RequestException as e:
        print(f"Request error while fetching data: {e}")
    except ValueError as e:
        print(f"Value error: {e}")
    return pd.DataFrame()  # Return empty DataFrame on failure


def transform(df):
    def process_dates_and_sort(df):
        df["date"] = pd.to_datetime(df["time"])
        df.drop(columns=["time"], inplace=True)
        df.sort_values(by="date", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)

    def calculate_temp_range(df):
        df["temp_range"] = df["temperature_2m_max"] - df["temperature_2m_min"]

    def define_nan_as_none(df):
        return df.where(pd.notnull(df), None)

    process_dates_and_sort(df)
    calculate_temp_range(df)
    return define_nan_as_none(df)


def load(df, path_to_db_file):
    with sq.connect(path_to_db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS historical_weather (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                precipitation FLOAT,
                windspeed FLOAT,
                temp_range FLOAT
            )
        """)
        for row in df.itertuples(index=False):
            try:
                cursor.execute("""
                    INSERT INTO historical_weather (
                        date, temp_max, temp_min, precipitation, windspeed, temp_range
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    row.date.strftime('%Y-%m-%d') if pd.notnull(row.date) else None,
                    float(row.temperature_2m_max) if pd.notnull(row.temperature_2m_max) else None,
                    float(row.temperature_2m_min) if pd.notnull(row.temperature_2m_min) else None,
                    float(row.precipitation_sum) if pd.notnull(row.precipitation_sum) else None,
                    float(row.windspeed_10m_max) if pd.notnull(row.windspeed_10m_max) else None,
                    float(row.temp_range) if pd.notnull(row.temp_range) else None
                ))
            except Exception as e:
                print(f"Error inserting row {row}: {e}")
        conn.commit()


def etl_historical_weather(latitude, longitude, path_to_db_file):
    try:
        df = extract(latitude, longitude)
        if df.empty:
            print("No data returned from extract(). Aborting ETL.")
            return
        df = transform(df)
        load(df, path_to_db_file)
        print(f"Historical weather data for coordinates ({latitude}, {longitude}) loaded into '{path_to_db_file}'")
    except Exception as e:
        print(f"ETL process failed: {e}")
