{{ config(materialized='view') }}

with src as (
  select
    wh.city_id,
    wh.payload
  from {{ source('raw','weather_hourly') }} wh
),

arr as (
  select
    s.city_id,
    (s.payload->'hourly'->'time')::jsonb                      as t_arr,
    (s.payload->'hourly'->'temperature_2m')::jsonb           as temp_arr,
    (s.payload->'hourly'->'relative_humidity_2m')::jsonb     as rh_arr,
    (s.payload->'hourly'->'precipitation')::jsonb            as pr_arr
  from src s
),

unnested as (
  select
    a.city_id,
    t.val::text                                        as ts_text_raw,      -- e.g. "\"2025-11-10T13:00\""
    temp.val::numeric                                  as temperature_c,
    rh.val::numeric                                    as humidity_pct,
    pr.val::numeric                                    as precipitation_mm
  from arr a
  join lateral jsonb_array_elements(a.t_arr)  with ordinality as t(val, i) on true
  left join lateral jsonb_array_elements(a.temp_arr) with ordinality as temp(val, j) on i=j
  left join lateral jsonb_array_elements(a.rh_arr)   with ordinality as rh(val, k)   on i=k
  left join lateral jsonb_array_elements(a.pr_arr)   with ordinality as pr(val, m)   on i=m
),

clean as (
  select
    city_id,
    replace(ts_text_raw, '"', '')                       as ts_text          -- now 2025-11-10T13:00 (no quotes)
  , temperature_c
  , humidity_pct
  , precipitation_mm
  from unnested
  where ts_text_raw is not null
)

select
  c.city_id,
  (to_timestamp(c.ts_text, 'YYYY-MM-DD"T"HH24:MI') at time zone 'UTC')            as observed_ts_utc,
  (to_timestamp(c.ts_text, 'YYYY-MM-DD"T"HH24:MI') at time zone 'UTC')::date      as observed_date,
  c.temperature_c,
  c.humidity_pct,
  c.precipitation_mm
from clean c