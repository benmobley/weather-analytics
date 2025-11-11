import os, datetime as dt, psycopg, requests, json
from dotenv import load_dotenv
load_dotenv()

PG_DSN = os.getenv("PG_DSN")

OPEN_METEO = (
  "https://api.open-meteo.com/v1/forecast?"
  "latitude={lat}&longitude={lon}"
  "&hourly=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"
  "&past_days=1&forecast_days=1&timezone=auto"
)

def fetch_for_city(cur, city_id, lat, lon):
    url = OPEN_METEO.format(lat=lat, lon=lon)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    payload = r.json()
    # figure date bounds from response
    times = payload.get("hourly", {}).get("time", [])
    date_from = min(times)[:10] if times else dt.date.today().isoformat()
    date_to   = max(times)[:10] if times else dt.date.today().isoformat()
    cur.execute(
        """
        insert into raw.weather_hourly (city_id, date_from, date_to, payload)
        values (%s, %s::date, %s::date, %s::jsonb)
        """,
        (city_id, date_from, date_to, json.dumps(payload))
    )

def main():
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("select city_id, lat, lon from core.dim_city where active")
            for city_id, lat, lon in cur.fetchall():
                fetch_for_city(cur, city_id, float(lat), float(lon))
    print("Ingest complete.")

if __name__ == "__main__":
    main()