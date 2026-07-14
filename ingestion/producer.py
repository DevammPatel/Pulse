#!/usr/bin/env python3
"""
NSE/BSE market-tick producer -> Kafka.

Design
------
Reliable free *real-time tick* APIs for Indian equities do not exist for
open/anonymous use, so this producer uses a hybrid model that is honest and
robust for a portfolio project:

  1. On startup it OPTIONALLY seeds a realistic last-traded price for each
     instrument from a public feed (Yahoo Finance via yfinance).
  2. It then generates a continuous stream of ticks with a geometric Brownian
     motion price walk + realistic bid/ask spread + Poisson-distributed trade
     sizes, sustaining a configurable throughput (TARGET_EPS, default 10,000/s).

This gives a genuine "10K+ events/sec, sub-second latency" pipeline to
demonstrate, while still grounding prices in real market values.

Throughput is achieved with the librdkafka-backed confluent-kafka client,
which comfortably exceeds 10K msg/s on a single core with batching.
"""
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from instruments import INSTRUMENTS, FALLBACK_PRICES

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market.ticks")
TARGET_EPS = int(os.getenv("TARGET_EPS", "10000"))
USE_LIVE_FEED = os.getenv("USE_LIVE_FEED", "true").lower() == "true"

_running = True


def _stop(*_):
    global _running
    _running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def seed_prices():
    """Return {symbol: last_price}. Try live feed, fall back to static seeds."""
    prices = dict(FALLBACK_PRICES)
    if not USE_LIVE_FEED:
        print("[producer] USE_LIVE_FEED=false -> using fallback seed prices", flush=True)
        return prices
    try:
        import yfinance as yf
        yahoo_symbols = [y for (_, y, _, _) in INSTRUMENTS]
        print("[producer] seeding live prices from Yahoo Finance...", flush=True)
        data = yf.download(
            tickers=" ".join(yahoo_symbols),
            period="1d", interval="1m",
            progress=False, threads=True,
        )
        # Map yahoo -> our symbol and pull the latest close
        for sym, yahoo, _exch, _sector in INSTRUMENTS:
            try:
                last = float(data["Close"][yahoo].dropna().iloc[-1])
                if last > 0:
                    prices[sym] = last
            except Exception:
                pass  # keep fallback for this one
        print(f"[producer] seeded {len(prices)} instruments from live feed", flush=True)
    except Exception as e:
        print(f"[producer] live seed failed ({e}); using fallback prices", flush=True)
    return prices


def build_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 20,               # batch aggressively for throughput
        "batch.num.messages": 50000,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 2_000_000,
        "queue.buffering.max.kbytes": 1_048_576,
        "acks": "1",
    })


def main():
    prices = seed_prices()
    # per-instrument volatility (annualised-ish, scaled down for tick cadence)
    vol = {sym: random.uniform(0.00005, 0.00025) for sym in prices}
    universe = [(s, y, e, sec) for (s, y, e, sec) in INSTRUMENTS if s in prices]

    producer = build_producer()
    print(
        f"[producer] -> {KAFKA_BOOTSTRAP} topic='{KAFKA_TOPIC}' "
        f"target={TARGET_EPS} eps, {len(universe)} instruments",
        flush=True,
    )

    sent = 0
    window_sent = 0
    window_start = time.time()
    # emit in small time-slices so we can pace to TARGET_EPS
    slice_seconds = 0.05
    per_slice = max(1, int(TARGET_EPS * slice_seconds))

    while _running:
        slice_begin = time.time()
        for _ in range(per_slice):
            sym, yahoo, exch, sector = random.choice(universe)
            p = prices[sym]
            # GBM step
            drift = -0.5 * vol[sym] ** 2
            shock = vol[sym] * random.gauss(0, 1)
            p = max(0.01, p * (2.718281828 ** (drift + shock)))
            prices[sym] = p

            spread = max(0.05, p * 0.0002)
            bid = round(p - spread / 2, 2)
            ask = round(p + spread / 2, 2)
            size = max(1, int(random.expovariate(1 / 250)))
            side = "BUY" if random.random() > 0.5 else "SELL"

            tick = {
                "symbol": sym,
                "exchange": exch,
                "sector": sector,
                "ltp": round(p, 2),          # last traded price
                "bid": bid,
                "ask": ask,
                "volume": size,
                "side": side,
                "event_time": datetime.now(timezone.utc).isoformat(),
            }
            producer.produce(
                KAFKA_TOPIC,
                key=sym.encode(),
                value=json.dumps(tick).encode(),
            )
            sent += 1
            window_sent += 1

        producer.poll(0)  # serve delivery callbacks / free queue

        # throughput report every ~5s
        now = time.time()
        if now - window_start >= 5.0:
            eps = window_sent / (now - window_start)
            print(f"[producer] sent={sent:,} last_window={eps:,.0f} eps", flush=True)
            window_sent = 0
            window_start = now

        # pace to target
        elapsed = time.time() - slice_begin
        if elapsed < slice_seconds:
            time.sleep(slice_seconds - elapsed)

    print("[producer] flushing...", flush=True)
    producer.flush(30)
    print(f"[producer] done. total sent={sent:,}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa
        print(f"[producer] fatal: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
