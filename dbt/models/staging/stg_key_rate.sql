select
    rate_date,
    rate,
    updated_at
from {{ source('dds', 'fct_key_rate') }}
