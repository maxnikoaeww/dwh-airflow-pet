-- Monthly statistics per currency: average, min/max, volatility, month-over-month change
with monthly as (
    select
        date_trunc('month', rate_date)::date as month,
        char_code,
        name,
        avg(vunit_rate)                      as avg_rate,
        min(vunit_rate)                      as min_rate,
        max(vunit_rate)                      as max_rate,
        stddev(change_pct)                   as volatility_pct,
        count(*)                             as trading_days,
        avg(key_rate)                        as avg_key_rate
    from {{ ref('currency_rate_daily') }}
    group by 1, 2, 3
)

select
    month,
    char_code,
    name,
    round(avg_rate, 6)                                                                 as avg_rate,
    min_rate,
    max_rate,
    round(volatility_pct, 4)                                                           as volatility_pct,
    trading_days,
    round(avg_key_rate, 2)                                                             as avg_key_rate,
    round(100 * (avg_rate / nullif(lag(avg_rate) over (partition by char_code order by month), 0) - 1), 4)
                                                                                       as mom_change_pct
from monthly
