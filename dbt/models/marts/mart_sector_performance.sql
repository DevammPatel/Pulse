-- Sector-level turnover & activity, latest trading day.
with d as (
    select * from {{ ref('mart_symbol_daily') }}
),
latest as (
    select max(event_date) as d from d
)
select
    d.event_date,
    d.sector,
    count(distinct d.symbol)                         as num_symbols,
    sum(d.day_turnover)                              as sector_turnover,
    sum(d.day_volume)                                as sector_volume,
    avg((d.day_close - d.day_open) / nullif(d.day_open, 0) * 100.0) as avg_pct_change,
    avg(d.avg_spread)                                as avg_spread
from d
join latest on d.event_date = latest.d
group by d.event_date, d.sector
order by sector_turnover desc
