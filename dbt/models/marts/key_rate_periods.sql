-- Periods of a constant key rate (gaps and islands)
with marked as (
    select
        rate_date,
        rate,
        case when rate is distinct from lag(rate) over (order by rate_date) then 1 else 0 end as is_change
    from {{ ref('stg_key_rate') }}
),

grouped as (
    select rate_date, rate, sum(is_change) over (order by rate_date) as period_no
    from marked
),

periods as (
    select
        period_no,
        min(rate_date) as date_from,
        max(rate_date) as date_to,
        min(rate)      as rate
    from grouped
    group by period_no
)

select
    period_no,
    date_from,
    date_to,
    date_to - date_from + 1                                   as days,
    rate,
    lag(rate) over (order by period_no)                       as prev_rate,
    ((rate - lag(rate) over (order by period_no)) * 100)::int as change_bps
from periods
