-- Metal prices in RUB per gram and in USD per troy ounce (31.1034768 g),
-- converted at the official USD rate in effect on the price date
with usd as (
    select rate_date, vunit_rate as usd_rate
    from {{ ref('currency_rate_daily') }}
    where char_code = 'USD'
)

select
    p.price_date,
    p.metal_code,
    m.metal_name,
    m.metal_name_en,
    p.buy                                                         as price_rub_per_gram,
    u.usd_rate,
    round(p.buy * 31.1034768 / u.usd_rate, 2)                     as price_usd_per_ounce,
    round(100 * (p.buy / nullif(lag(p.buy) over w, 0) - 1), 4)    as change_pct
from {{ ref('stg_metal_price') }} as p
join {{ ref('metal_codes') }} as m
    on m.metal_code = p.metal_code
left join lateral (
    select usd.usd_rate
    from usd
    where usd.rate_date <= p.price_date
    order by usd.rate_date desc
    limit 1
) as u on true
window w as (partition by p.metal_code order by p.price_date)
