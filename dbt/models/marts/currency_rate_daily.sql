{#
  Incremental: rebuilds every date from the earliest one whose source rows changed
  since the last build (backfill of old dates and late corrections included).
  The 1 hour margin covers loads that committed while the previous build was running.
  Window functions look 60 days further back so moving averages stay correct.
#}
{{ config(
    materialized='incremental',
    unique_key=['rate_date', 'currency_id'],
    incremental_strategy='delete+insert'
) }}

{% if is_incremental() %}
{% set changed_from %}
(
    select coalesce(min(rate_date), current_date + 1)
    from (
        select rate_date from {{ ref('stg_currency_rate') }}
        where updated_at > (select max(updated_at) - interval '1 hour' from {{ this }})
        union all
        select rate_date from {{ ref('stg_key_rate') }}
        where updated_at > (select max(updated_at) - interval '1 hour' from {{ this }})
    ) as changed
)
{% endset %}
{% endif %}

with base as (
    select
        r.rate_date,
        r.currency_id,
        coalesce(d.char_code, r.char_code) as char_code,
        coalesce(d.name, r.name)           as name,
        d.eng_name,
        r.nominal,
        r.value,
        r.vunit_rate
    from {{ ref('stg_currency_rate') }} as r
    left join {{ ref('stg_dim_currency') }} as d      -- as-of join to the SCD2 version
        on d.currency_id = r.currency_id
       and r.rate_date >= d.valid_from
       and r.rate_date <  d.valid_to
    {% if is_incremental() %}
    where r.rate_date >= {{ changed_from }} - 60
    {% endif %}
),

calc as (
    select
        b.*,
        lag(vunit_rate) over w                                             as prev_vunit_rate,
        avg(vunit_rate) over (w rows between 6 preceding and current row)  as ma_7,
        avg(vunit_rate) over (w rows between 29 preceding and current row) as ma_30
    from base as b
    window w as (partition by currency_id order by rate_date)
)

select
    c.rate_date,
    c.currency_id,
    c.char_code,
    c.name,
    c.eng_name,
    c.nominal,
    c.value,
    c.vunit_rate,
    c.prev_vunit_rate,
    c.vunit_rate - c.prev_vunit_rate                                     as change_abs,
    round(100 * (c.vunit_rate / nullif(c.prev_vunit_rate, 0) - 1), 4)    as change_pct,
    round(c.ma_7, 8)                                                     as ma_7,
    round(c.ma_30, 8)                                                    as ma_30,
    k.rate                                                               as key_rate,
    now()                                                                as updated_at
from calc as c
left join lateral (
    select kr.rate
    from {{ ref('stg_key_rate') }} as kr
    where kr.rate_date <= c.rate_date
    order by kr.rate_date desc
    limit 1
) as k on true
{% if is_incremental() %}
where c.rate_date >= {{ changed_from }}
{% endif %}
