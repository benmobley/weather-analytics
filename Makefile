up: ; docker compose up -d
ingest: ; source .venv/bin/activate && python etl/fetch_weather.py
dbt: ; cd weather_dbt && DBT_PROFILES_DIR=$(pwd) dbt run && dbt test