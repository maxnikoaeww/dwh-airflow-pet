-- Singular test: change_abs must equal vunit_rate - prev_vunit_rate
select rate_date, currency_id
from {{ ref('currency_rate_daily') }}
where prev_vunit_rate is not null
  and abs(change_abs - (vunit_rate - prev_vunit_rate)) > 0.00000001
