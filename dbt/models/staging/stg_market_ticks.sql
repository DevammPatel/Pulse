-- Lightweight typed view over the silver ticks; the clean entry point for marts.
select
    symbol,
    exchange,
    sector,
    cast(ltp as double)         as ltp,
    cast(bid as double)         as bid,
    cast(ask as double)         as ask,
    cast(volume as bigint)      as volume,
    cast(spread as double)      as spread,
    cast(trade_value as double) as trade_value,
    side,
    event_time,
    event_date
from {{ source('market', 'silver_market_ticks') }}
where ltp > 0
  and ask >= bid
