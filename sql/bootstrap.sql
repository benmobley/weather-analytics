create schema if not exists raw;
create schema if not exists core;
create schema if not exists staging;
create schema if not exists marts;

-- city catalog (so you can add more later)
create table if not exists core.dim_city (
  city_id serial primary key,
  city_name text not null,
  country text not null,
  lat numeric not null,
  lon numeric not null,
  timezone text not null default 'America/Chicago',
  active boolean not null default true
);

-- raw land (1 row per city per fetch; JSON columns keep you flexible)
create table if not exists raw.weather_hourly (
  ingest_ts timestamptz default now(),
  city_id int not null references core.dim_city(city_id),
  date_from date not null,
  date_to date not null,
  payload jsonb not null
);

-- seed one city (Chicago); add more rows later
insert into core.dim_city (city_name, country, lat, lon)
values ('Chicago','US',41.8781,-87.6298)
on conflict do nothing;