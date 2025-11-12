select
  a.city_id,
  c.city_name,
  a.date,
  lower(a.parameter) as parameter,
  a.value::numeric as value,
  a.unit
from {{ source('raw','air_quality') }} a
join {{ source('core','dim_city') }} c using (city_id)