{{ config(materialized='table') }}

with hourly as (
  select
    city_id,
    date(ts_utc) as d,
    temp_c,
    rh_pct,
    precipitation_mm,
    wind_speed
  from {{ ref('stg_weather_hourly') }}
)
select
  city_id,
  d as date,
  avg(temp_c)              as avg_temp_c,
  min(temp_c)              as min_temp_c,
  max(temp_c)              as max_temp_c,
  avg(rh_pct)              as avg_rh_pct,
  sum(precipitation_mm)    as total_precip_mm,
  avg(wind_speed)          as avg_wind_speed
from hourly
group by 1,2