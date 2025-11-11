{{ config(materialized='view') }}

select
  city_id,
  (ts)::timestamp at time zone 'UTC' as ts_utc,
  temp_c,
  rh_pct,
  precipitation_mm,
  wind_speed
from (
  select
    wh.city_id,
    t.value::text                      as ts,
    (temps.value)::text::float         as temp_c,
    (rh.value)::text::float            as rh_pct,
    (prec.value)::text::float          as precipitation_mm,
    (wind.value)::text::float          as wind_speed
  from raw.weather_hourly wh
  cross join lateral jsonb_array_elements(wh.payload->'hourly'->'time')
    with ordinality as t(value, ord)
  cross join lateral jsonb_array_elements(wh.payload->'hourly'->'temperature_2m')
    with ordinality as temps(value, ord2)
  cross join lateral jsonb_array_elements(wh.payload->'hourly'->'relative_humidity_2m')
    with ordinality as rh(value, ord3)
  cross join lateral jsonb_array_elements(wh.payload->'hourly'->'precipitation')
    with ordinality as prec(value, ord4)
  cross join lateral jsonb_array_elements(wh.payload->'hourly'->'wind_speed_10m')
    with ordinality as wind(value, ord5)
  where ord = ord2 and ord = ord3 and ord = ord4 and ord = ord5
) e