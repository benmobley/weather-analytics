-- Ensure unique constraint exists (safe to run)
do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conrelid = 'core.dim_city'::regclass
      and contype = 'u'
      and conname = 'uq_city'
  ) then
    alter table core.dim_city add constraint uq_city unique (city_name, country);
  end if;
end$$;

-- Add cities (idempotent)
insert into core.dim_city (city_name, country, lat, lon, timezone, active) values
  ('New York','US',40.7128,-74.0060,'America/New_York', true),
  ('Los Angeles','US',34.0522,-118.2437,'America/Los_Angeles', true),
  ('London','GB',51.5074,-0.1278,'Europe/London', true),
  ('Paris','FR',48.8566,2.3522,'Europe/Paris', true),
  ('Tokyo','JP',35.6762,139.6503,'Asia/Tokyo', true)
on conflict (city_name, country) do nothing;