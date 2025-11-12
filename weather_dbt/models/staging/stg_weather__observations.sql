select
  city_id,
  observed_date as date,
  avg(temperature_c)        as avg_temp_c,
  min(temperature_c)        as min_temp_c,
  max(temperature_c)        as max_temp_c,
  avg(humidity_pct)         as avg_humidity_pct,
  sum(precipitation_mm)     as precipitation_mm
from {{ ref('stg_weather__hourly') }}
group by 1, 2