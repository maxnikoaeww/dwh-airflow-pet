select
    rate_date,
    currency_id,
    char_code,
    name,
    nominal,
    value,
    vunit_rate,
    updated_at
from {{ source('dds', 'fct_currency_rate') }}
