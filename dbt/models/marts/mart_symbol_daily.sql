-- Daily rollup per symbol from the 1-minute candles.
with c as (
    select * from {{ ref('stg_ohlc_1min') }}
)
select
    event_date,
    symbol,
    exchange,
    sector,
    min(low)                                as day_low,
    max(high)                               as day_high,
    element_at(array_agg(open  order by window_start asc), 1)  as day_open,
    element_at(array_agg(close order by window_start desc), 1) as day_close,
    sum(volume)                             as day_volume,
    sum(turnover)                           as day_turnover,
    sum(tick_count)                         as day_ticks,
    avg(avg_spread)                         as avg_spread
from c
group by event_date, symbol, exchange, sector
