# ETL Pipelines for Financial and Weather Data

This project contains two independent ETL pipelines:

1. 📈 `1_csv_ETL_pipeline`: Extracts, transforms, and loads historical ETF (Exchange-Traded Fund) price data from a CSV file.  
2. 🌦️ `2_api_ETL_pipeline`: Extracts, transforms, and loads weather data (both current and historical) from the [Open-Meteo API](https://open-meteo.com/).

## 🔧 Requirements

Install dependencies using:

```bash
pip install -r requirements.txt
```

## 1️⃣ ETF CSV ETL Pipeline

**Location**: `1_csv_ETL_pipeline/etl_from_csv.py`

**Description**:
- Extracts data from a CSV file.
- Cleans and transforms columns (`Price`, `Volume`, `Change %`, etc.).
- Calculates indicators:
  - SMA (Simple Moving Average)
  - EMA (Exponential Moving Average)
- Loads the cleaned and enriched data into a SQLite database.

**Usage**:

```bash
python 1_csv_ETL_pipeline/etl_from_csv.py path/to/file.csv output_db_name
```

**Example**:

```bash
python 1_csv_ETL_pipeline/etl_from_csv.py 1_csv_ETL_pipeline/pure_data/example_etf_data.csv etf_output
```

The resulting database will be saved in `cleaned_data/etf_output.db`.

---

## 2️⃣ Weather API ETL Pipeline

**Location**: `2_api_ETL_pipeline/etl_from_api.py`

This pipeline includes two scripts:
- `etl_current_weather.py`: Extracts current weather data.
- `etl_historical_weather.py`: Extracts weather data for the last 365 days.
- `etl_from_api.py`: Main launcher that runs both scripts.

**Features**:
- Extracts data from [Open-Meteo](https://open-meteo.com/) APIs.
- Transforms the data:
  - Converts date columns.
  - Calculates daily temperature range.
- Loads both datasets into a SQLite database.

**Usage**:

```bash
python 2_api_ETL_pipeline/etl_from_api.py <database_name> <latitude> <longitude> [--num_of_calls N] [--time_interval MINUTES]
```

**Example** (for Paris, France):

```bash
python 2_api_ETL_pipeline/etl_from_api.py weather_data 48.85 2.35 --num_of_calls 3 --time_interval 60
```

Creates a SQLite database at `cleaned_data/weather_data.db` with the following tables:
- `current_weather`
- `historical_weather`

---

## ✍️ Author

**Gabriel MORENO REYES**  
📍 Based in France  
🎓 Étudiant à l’école 42 / Mathematician

---

## 🧭 Notes

- This is an educational project to practice and demonstrate ETL design and data handling with Python.
- It can be extended to include analytics, dashboards, or integration with tools like Airflow.

---

## ✅ To-Do

- Add unit tests
- Add Docker support
- Add scheduler (e.g. with cron or Airflow)

