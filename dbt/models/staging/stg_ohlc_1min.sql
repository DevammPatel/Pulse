-- Typed view over the gold 1-minute candles.
select
    window_start,
    window_end,
    symbol,
    exchange,
    sector,
    cast(open  as double) as open,
    cast(high  as double) as high,
    cast(low   as double) as low,
    cast(close as double) as close,
    cast(volume as bigint) as volume,
    cast(turnover as double) as turnover,
    cast(tick_count as bigint) as tick_count,
    cast(avg_spread as double) as avg_spread,
    event_date
from {{ source('market', 'gold_ohlc_1min') }}
