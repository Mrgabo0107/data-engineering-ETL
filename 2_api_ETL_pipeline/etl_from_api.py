# import requests
# import datetime
# import pandas as pd
from argparse import ArgumentTypeError, ArgumentParser
from os import makedirs
from etl_historical_weather import etl_historical_weather
from etl_current_weather import etl_current_weather
# import time
# Coordenadas de París
# latitude = 48.8547
# longitude = 2.3475


    


def init():
    def positive_int(value):
        ivalue = int(value)
        if ivalue <= 0:
            raise ArgumentTypeError(f"{value} is not a positive integer")
        return ivalue
    def positive_float(value):
        fvalue = float(value)
        if fvalue <= 0:
            raise ArgumentTypeError(f"{value} is not a positive float")
        return fvalue
    parser = ArgumentParser(description='ETL pipeline for weather data')
    parser.add_argument("database_name", type=str, help="Name of the database to save")
    parser.add_argument("latitude", type=float, help="Latitude of the location")
    parser.add_argument("longitude", type=float, help="Longitude of the location")
    parser.add_argument("--num_of_calls", type=positive_int, default = 10, help="Number of times current weather info will be requested (default: 10)")
    parser.add_argument("--time_interval", type=positive_float, default=60, help="Time interval between requests(in minutes, default: 60,  recommended: > 60))")
    args = parser.parse_args()
    database_dir = "cleaned_data"
    path_to_database_file = f"{database_dir}/{args.database_name}.db"
    makedirs(database_dir, exist_ok=True)
    return path_to_database_file, args.latitude, args.longitude, args.num_of_calls, args.time_interval

if __name__ == "__main__":

    path_to_database_file, latitude, longitude, numb_call, t_interval= init()
    etl_historical_weather(latitude, longitude, path_to_database_file)
    etl_current_weather(latitude, longitude, numb_call, t_interval, path_to_database_file)