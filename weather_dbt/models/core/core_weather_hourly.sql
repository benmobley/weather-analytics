{{ config(materialized='view') }}

select
  c.city_name,
  c.country,
  c.timezone,
  s.ts_utc at time zone c.timezone as ts_local,
  s.*
from {{ ref('stg_weather_hourly') }} s
join core.dim_city c on c.city_id = s.city_id