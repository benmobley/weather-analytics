#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/Desktop/data_projects/weather_analytics"
source .venv/bin/activate
export $(grep -v '^#' .env | xargs) 
python etl/fetch_weather.py >> logs/weather.log 2>&1