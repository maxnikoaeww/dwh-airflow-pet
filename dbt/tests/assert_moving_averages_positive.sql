-- Singular test: moving averages of positive rates must be positive
select rate_date, currency_id
from {{ ref('currency_rate_daily') }}
where ma_7 <= 0 or ma_30 <= 0
