with weather as (
  select city_id, date, avg_temp_c, min_temp_c, max_temp_c, avg_humidity_pct, precipitation_mm
  from {{ ref('stg_weather__observations') }}
),
aq as (
  select
    city_id,
    date,
    max(case when parameter='pm25' then value end) as pm25,
    max(case when parameter='pm10' then value end) as pm10,
    max(case when parameter='o3'   then value end) as o3,
    max(case when parameter='no2'  then value end) as no2
  from {{ ref('stg_aq__measurements') }}
  group by 1,2
)
select
  coalesce(w.city_id, a.city_id) as city_id,
  coalesce(w.date, a.date) as date,
  w.avg_temp_c, w.min_temp_c, w.max_temp_c, w.avg_humidity_pct, w.precipitation_mm,
  a.pm25, a.pm10, a.o3, a.no2
from weather w
full outer join aq a
  on w.city_id = a.city_id and w.date = a.date