select
    currency_id,
    char_code,
    name,
    eng_name,
    valid_from,
    valid_to,
    is_current
from {{ source('dds', 'dim_currency') }}
