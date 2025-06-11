import requests
import datetime
import pandas as pd
import argparse
import os
# Coordenadas de París
# latitude = 48.8547
# longitude = 2.3475

def extract_historical_data(latitude, longitude):
    historic_df = pd.DataFrame()
    url = "https://archive-api.open-meteo.com/v1/archive"
    today = datetime.date.today()
    one_year_ago = today - datetime.timedelta(days=365)
    start_date = one_year_ago.isoformat()
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
            raise ValueError("Error: no historic info in API response")
        historic_df = pd.DataFrame(data=data["daily"], index=None)
    except requests.exceptions.RequestException as e:
        print(f"Error getting historic data: {e}")
    except ValueError as e:
        print(f"Error getting historic data: {e}")
    return historic_df
    

def transform(df):
    def process_dates_and_sort(df):
        df["date"] = pd.to_datetime(df["time"])
        df.drop(columns=["time"], inplace=True)
        df.sort_values(ascending=False, by="date", inplace=True)
        df.reset_index(drop=True, inplace=True)
    def calculate_temp_range(df):
        df["temp_range"] = df["temperature_2m_max"] - df["temperature_2m_min"]

    print_data_f("antes",df)    
    process_dates_and_sort(df)
    calculate_temp_range(df)
    print_data_f("despues",df)    

def print_data_f(message,df):
    print (f"\n------------\n{message}")
    print(df)
    print(df.dtypes)


def init():
    parser = argparse.ArgumentParser(description='ETL pipeline for weather data')
    parser.add_argument("database_name", type=str, help="Name of the database to save")
    parser.add_argument("latitude", type=float, help="Latitude of the location")
    parser.add_argument("longitude", type=float, help="Longitude of the location")

    args = parser.parse_args()
    database_dir = "cleaned_data"
    path_to_database_file = f"{database_dir}/{args.database_name}.db"
    os.makedirs(database_dir, exist_ok=True)
    return path_to_database_file, args.latitude, args.longitude

if __name__ == "__main__":
    path_to_database_file, latitude, longitude = init()
    historical_df = extract_historical_data(latitude,longitude)
    if not historical_df.empty:
        transform(historical_df)