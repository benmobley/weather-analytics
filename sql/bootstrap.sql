create schema if not exists raw;
create schema if not exists core;
create schema if not exists staging;
create schema if not exists marts;

create table if not exists core.dim_city (
  city_id   serial primary key,
  city_name text    not null,
  country   text    not null,
  lat       numeric not null,
  lon       numeric not null,
  timezone  text    not null default 'America/Chicago',
  active    boolean not null default true,
  constraint uq_city unique (city_name, country)
);

create table if not exists raw.weather_hourly (
  ingest_ts timestamptz default now(),
  city_id   int not null references core.dim_city(city_id),
  date_from date not null,
  date_to   date not null,
  payload   jsonb not null
);

create table if not exists raw.air_quality (
  ingest_ts timestamptz default now(),
  city_id   int  not null references core.dim_city(city_id),
  date      date not null,
  parameter text not null,     -- pm25, pm10, o3, no2
  value     numeric,
  unit      text,
  source    text not null default 'open-meteo',
  constraint pk_air_quality primary key (city_id, date, parameter)
);

create index if not exists ix_air_quality_date on raw.air_quality(date);

-- Seeds (safe to re-run)
insert into core.dim_city (city_name, country, lat, lon)
values ('Chicago','US',41.8781,-87.6298)
on conflict (city_name, country) do nothing;

insert into core.dim_city (city_name, country, lat, lon, timezone)
values
  ('Berlin','DE',52.5200,13.4050,'Europe/Berlin'),
  ('Barcelona','ES',41.3874,2.1686,'Europe/Madrid')
on conflict (city_name, country) do nothing;