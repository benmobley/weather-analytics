{{ config(materialized='view') }}

select
  c.city_id,
  c.city_name,
  c.country,
  c.timezone,
  s.observed_ts_utc,
  (s.observed_ts_utc at time zone c.timezone) as ts_local,
  s.observed_date,
  s.temperature_c,
  s.humidity_pct,
  s.precipitation_mm
from {{ ref('stg_weather__hourly') }} s
join {{ source('core','dim_city') }} c using (city_id)