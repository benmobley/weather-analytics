#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/Desktop/data_projects/weather_analytics"
source .venv/bin/activate
export $(grep -v '^#' .env | xargs)
python etl/fetch_air_quality.py >> logs/air_quality.log 2>&1