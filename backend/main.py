import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pandas as pd
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync

SSID = os.environ.get("POCKET_OPTION_SSID")
SYMBOLS = ["EURHUF_otc", "EURUSD_otc"]
PERIOD = 15            # 15-second candles
N_CLOSED = 10          # last N closed candles to keep
MIN_TICKS = 3          # drop buckets with fewer ticks
REPOLL_SECONDS = 20    # how often to recalculate

candle_store = {s: [] for s in SYMBOLS}
subscribers: list[WebSocket] = []

# We will hold raw ticks in memory for resampling
tick_store = {s: [] for s in SYMBOLS}


async def stream_ticks(client: PocketOptionAsync, symbol: str):
    """Continuously ingest ticks into memory."""
    print(f"[{symbol}] started tick ingestion...")
    try:
        # hours=0.1 and max_rows=2 ensures we only get the live tick stream, not historical compile
        async for closed, forming in client.get_candles_live(symbol, period=PERIOD, hours=0.1, max_rows=2):
            # Both 'closed' and 'forming' contain price data. We want the ticks.
            # We'll just grab the timestamp and close price.
            if forming and "time" in forming and "close" in forming:
                tick_store[symbol].append({
                    "time": forming["time"],
                    "price": float(forming["close"])
                })
            
            # Keep memory bounded (max 5000 ticks per asset)
            if len(tick_store[symbol]) > 5000:
                tick_store[symbol] = tick_store[symbol][-5000:]

            await asyncio.sleep(0.05)
    except Exception as e:
        print(f"[{symbol}] tick stream error: {e!r}")


async def calculate_closed_candles():
    """Resample stored ticks into 15s OHLC and drop the forming bucket."""
    for symbol in SYMBOLS:
        ticks = tick_store[symbol]
        if not ticks:
            print(f"[{symbol}] waiting for ticks...")
            continue
            
        try:
            df = pd.DataFrame(ticks)
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df = df.set_index("time").sort_index()
            
            # Resample
            ohlc = df["price"].resample(f"{PERIOD}s", label="left", closed="left").agg(
                open="first", high="max", low="min", close="last", count="count"
            )
            
            # Drop forming bucket
            now = pd.Timestamp.now(tz="UTC")
            bucket_end = ohlc.index + pd.Timedelta(seconds=PERIOD)
            ohlc = ohlc[bucket_end <= now]
            
            # Drop sparse buckets
            ohlc = ohlc[ohlc["count"] >= MIN_TICKS]
            ohlc = ohlc.tail(N_CLOSED)
            
            converted = []
            for ts, row in ohlc.iterrows():
                converted.append({
                    "time": int(ts.timestamp()),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                })
                
            candle_store[symbol] = converted
            
            if converted:
                print(f"[{symbol}] resampled {len(converted)} closed candles from {len(ticks)} ticks")
                msg = json.dumps({"symbol": symbol, "history": converted})
                for ws in subscribers[:]:
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        if ws in subscribers:
                            subscribers.remove(ws)

        except Exception as e:
            print(f"[{symbol}] resample error: {e!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID environment variable is not set")
    client = PocketOptionAsync(SSID)
    await asyncio.sleep(5)

    # Start tick ingestion
    tick_tasks = [asyncio.create_task(stream_ticks(client, s)) for s in SYMBOLS]

    # Start periodic resampling
    async def resample_loop():
        while True:
            await asyncio.sleep(REPOLL_SECONDS)
            await calculate_closed_candles()

    resample_task = asyncio.create_task(resample_loop())
    
    # Trigger an immediate first resample just in case ticks are already buffered
    asyncio.create_task(calculate_closed_candles())

    print("[lifespan] tick ingestion and resampling loop started")
    yield
    
    for t in tick_tasks:
        t.cancel()
    resample_task.cancel()
    print("[lifespan] shutdown")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    detail = {}
    for s in SYMBOLS:
        c = candle_store[s]
        detail[s] = {
            "ticks_buffered": len(tick_store[s]),
            "candles_ready": len(c),
            "last_epoch": c[-1]["time"] if c else None,
            "last_close": c[-1]["close"] if c else None,
        }
    return JSONResponse({"status": "ok", "symbols": detail})


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    subscribers.append(ws)
    try:
        for symbol, candles in candle_store.items():
            if candles:
                await ws.send_text(json.dumps({"symbol": symbol, "history": candles}))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in subscribers:
            subscribers.remove(ws)


app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
