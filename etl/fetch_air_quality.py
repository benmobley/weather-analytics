# etl/fetch_air_quality.py
import os
import datetime
import math
import requests
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

# ------------------------------------------------------------
# Load env
# ------------------------------------------------------------
load_dotenv()
PG_DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/weather")

# We’ll pull yesterday’s air quality and store daily averages
PARAMS = {
    "pm25":  "pm2_5",             # μg/m³
    "pm10":  "pm10",              # μg/m³
    "o3":    "ozone",             # μg/m³ (Open-Meteo returns μg/m³)
    "no2":   "nitrogen_dioxide",  # μg/m³
}
UNITS = {  # Open-Meteo docs
    "pm25": "µg/m³",
    "pm10": "µg/m³",
    "o3":   "µg/m³",
    "no2":  "µg/m³",
}

BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

def get_active_cities(conn):
    with conn.cursor() as cur:
        cur.execute("""
            select city_id, city_name, country, lat, lon
            from core.dim_city
            where active = true
        """)
        return [dict(r) for r in cur.fetchall()]

def fetch_city_day(lat, lon, day):
    """Fetch hourly AQ for a single day, return dict param -> list[float]."""
    hourly_list = ",".join(PARAMS.values())
    q = {
        "latitude":  f"{lat:.5f}",
        "longitude": f"{lon:.5f}",
        "hourly":    hourly_list,
        "start_date": day.isoformat(),
        "end_date":   day.isoformat(),
        "timezone":  "UTC",
    }
    r = requests.get(BASE_URL, params=q, timeout=30)
    r.raise_for_status()
    data = r.json().get("hourly", {})
    # Build arrays per logical param name
    out = {}
    for logical, api_name in PARAMS.items():
        out[logical] = data.get(api_name, []) or []
    return out

def avg(values):
    vals = [float(v) for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    return round(sum(vals) / len(vals), 3) if vals else None

def upsert_rows(conn, rows):
    sql = """
    insert into raw.air_quality (city_id, date, parameter, value, unit, source)
    values (%(city_id)s, %(date)s, %(parameter)s, %(value)s, %(unit)s, 'open-meteo')
    on conflict (city_id, date, parameter) do update
      set value = excluded.value,
          unit  = excluded.unit,
          ingest_ts = now();
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)

def main():
    day = datetime.date.today() - datetime.timedelta(days=1)

    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn:
        cities = get_active_cities(conn)

    to_upsert = []
    for c in cities:
        print(f"Fetching AQ for {c['city_name']} on {day}...")
        try:
            hourly = fetch_city_day(c["lat"], c["lon"], day)
        except requests.HTTPError as e:
            print(f"  Skipping {c['city_name']}: {e}")
            continue

        for p in PARAMS.keys():
            value = avg(hourly.get(p, []))
            if value is None:
                continue
            to_upsert.append({
                "city_id": c["city_id"],
                "date": day,
                "parameter": p,        # pm25 / pm10 / o3 / no2
                "value": value,
                "unit": UNITS[p],
            })

    if not to_upsert:
        print(f"No AQ data loaded for {day}")
        return

    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn:
        upsert_rows(conn, to_upsert)
        conn.commit()

    print(f"✅ Loaded {len(to_upsert)} air-quality rows for {day}")

if __name__ == "__main__":
    main()