select
    price_date,
    metal_code,
    buy,
    sell,
    updated_at
from {{ source('dds', 'fct_metal_price') }}
