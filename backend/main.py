import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync

SSID = os.environ.get("POCKET_OPTION_SSID")
SYMBOLS = ["EURHUF_otc", "EURUSD_otc"]
TIMEFRAME = 15  # seconds
HISTORY_COUNT = 99

# In-memory candle store: { symbol: [candles...] }
candle_store = {s: [] for s in SYMBOLS}
subscribers = []

def candle_to_lwc(candle):
    """Map library candle dict to Lightweight Charts format.
    Library may return 'time' as ISO string or epoch; adjust as needed."""
    t = candle.get("time")
    if isinstance(t, str):
        # Parse ISO or similar to epoch seconds
        dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        epoch = int(dt.timestamp())
    else:
        epoch = int(t)
    return {
        "time": epoch,
        "open": float(candle["open"]),
        "high": float(candle["high"]),
        "low": float(candle["low"]),
        "close": float(candle["close"]),
    }

async def stream_symbol(client: PocketOptionAsync, symbol: str):
    """Backfill history, then stream live 15s candles."""
    # 1. Backfill 99 candles
    try:
        hist = await client.get_candles(symbol, TIMEFRAME, HISTORY_COUNT)
        for c in hist:
            candle_store[symbol].append(candle_to_lwc(c))
        print(f"[{symbol}] Backfilled {len(hist)} candles")
    except Exception as e:
        print(f"[{symbol}] Backfill failed: {e}")

    # 2. Stream live (try timed first; fall back to chunked)
    try:
        stream = await client.subscribe_symbol_timed(symbol, 15)  # 15 SECONDS
    except AttributeError:
        # Older version: timed method may not exist; try chunked as fallback
        stream = await client.subscribe_symbol_chunked(symbol, 15)  # 15 TICKS (not seconds!)

    async for candle in stream:
        lwc = candle_to_lwc(candle)
        store = candle_store[symbol]
        # Update or append based on time
        if store and store[-1]["time"] == lwc["time"]:
            store[-1] = lwc  # Update forming candle
        else:
            store.append(lwc)
            if len(store) > 500:  # Keep memory bounded
                store.pop(0)
        # Broadcast to WebSocket subscribers
        msg = json.dumps({"symbol": symbol, "candle": lwc})
        for ws in subscribers:
            try:
                await ws.send_text(msg)
            except Exception:
                pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID env var not set")
    client = PocketOptionAsync(ssid=SSID)
    await client.__aenter__()
    # Launch one stream task per symbol
    tasks = [asyncio.create_task(stream_symbol(client, s)) for s in SYMBOLS]
    yield
    for t in tasks:
        t.cancel()
    await client.__aexit__(None, None, None)

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    subscribers.append(ws)
    try:
        # Send existing history immediately
        for symbol, candles in candle_store.items():
            await ws.send_text(json.dumps({
                "symbol": symbol,
                "history": candles[-HISTORY_COUNT:]
            }))
        while True:
            await ws.receive_text()  # Keep alive
    except WebSocketDisconnect:
        subscribers.remove(ws)

# Serve frontend
app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
