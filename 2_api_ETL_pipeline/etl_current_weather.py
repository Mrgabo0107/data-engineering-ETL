import time
import requests
import pandas as pd
import sqlite3 as sq
from utils import print_data_f
from datetime import datetime

def extract(latitude, longitude):


    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m,precipitation,windspeed_10m",
        "timezone": "auto"
        }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if "current" not in data:
            raise ValueError("No 'current' key found in API response.")
        return pd.DataFrame([data["current"]])
    except requests.exceptions.RequestException as e:
        print(f"Request error while fetching data: {e}")
    except ValueError as e:
        print(f"Value error: {e}")
    return pd.DataFrame()

def transform(df):
    def process_dates_and_sort(df):
        df["date"] = pd.to_datetime(df["time"])
        df.drop(columns=["time"], inplace=True)
        df.drop(columns=["interval"], inplace=True)
        df.sort_values(by="date", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)

    def define_nan_as_none(df):
        return df.where(pd.notnull(df), None)

    process_dates_and_sort(df)
    return define_nan_as_none(df)

def load(df, path_to_db_file):
    with sq.connect(path_to_db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS current_weather (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                datetime DATE,
                temp FLOAT,
                precipitation FLOAT,
                windspeed FLOAT
            )
        """)
        for row in df.itertuples(index=False):
            try:
                cursor.execute("""
                    INSERT INTO current_weather (
                        datetime, temp, precipitation, windspeed
                    ) VALUES (?, ?, ?, ?)
                """, (
                    row.date.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row.date) else None,
                    float(row.temperature_2m) if pd.notnull(row.temperature_2m) else None,
                    float(row.precipitation) if pd.notnull(row.precipitation) else None,
                    float(row.windspeed_10m) if pd.notnull(row.windspeed_10m) else None,
                ))
            except Exception as e:
                print(f"Error inserting row {row}: {e}")
        conn.commit()


def etl_current_weather(latitude, longitude, numb_call, t_interval, path_to_db_file):
    for i in range(numb_call):
        try:
            df = extract(latitude, longitude)
            if df is not None and not df.empty:
                df = transform(df)
                load(df, path_to_db_file)
                print(f"{i+1} {datetime.now().strftime('%H:%M:%S')}: Current weather data for coordinates ({latitude}, {longitude}) loaded into {path_to_db_file}")
            else:
                print(f"No data received in call {i+1}")
        except Exception as e:
            print(f"ETL process failed at call {i+1}: {e}")
        
        if i < numb_call - 1:
            time.sleep(t_interval * 60) 