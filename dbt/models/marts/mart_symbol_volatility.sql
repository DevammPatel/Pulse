-- Rolling intraday volatility & range per symbol from 1-min candles.
with c as (
    select * from {{ ref('stg_ohlc_1min') }}
)
select
    event_date,
    symbol,
    sector,
    count(*)                                    as minutes_traded,
    avg((high - low) / nullif(low, 0) * 100.0)  as avg_minute_range_pct,
    stddev_pop((close - open) / nullif(open, 0) * 100.0) as return_volatility_pct,
    max(high)                                   as high,
    min(low)                                    as low,
    sum(volume)                                 as volume
from c
group by event_date, symbol, sector
