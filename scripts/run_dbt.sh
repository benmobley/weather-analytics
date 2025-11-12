#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/Desktop/data_projects/weather_analytics/weather_dbt"
dbt run >> ../logs/dbt_run.log 2>&1
dbt test >> ../logs/dbt_test.log 2>&1