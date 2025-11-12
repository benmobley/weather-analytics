{{ config(materialized='table') }}

with hourly as (
  select
    city_id,
    observed_date as date,      
    temperature_c,
    humidity_pct,
    precipitation_mm
  from {{ ref('stg_weather__hourly') }}
)
select
  city_id,
  date,
  avg(temperature_c)    as avg_temp_c,
  min(temperature_c)    as min_temp_c,
  max(temperature_c)    as max_temp_c,
  avg(humidity_pct)     as avg_humidity_pct,
  sum(precipitation_mm) as total_precip_mm
from hourly
group by 1,2