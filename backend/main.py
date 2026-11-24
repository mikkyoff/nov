import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync

SSID = os.environ.get("POCKET_OPTION_SSID")
SYMBOLS = ["EURHUF_otc", "EURUSD_otc"]
TIMEFRAME = 15
HISTORY_COUNT = 100

candle_store = {s: [] for s in SYMBOLS}
subscribers: list[WebSocket] = []


def candle_to_lwc(candle):
    t = candle.get("time")
    if isinstance(t, str):
        try:
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
            epoch = int(dt.timestamp())
        except ValueError:
            dt = datetime.strptime(t, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
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


async def poll_historical(client: PocketOptionAsync):
    """Poll the last 100 candles for both assets using candles_many."""
    try:
        feed = await client.candles_many(SYMBOLS, TIMEFRAME, count=HISTORY_COUNT)
        for symbol, candles in feed.items():
            if candles:
                converted = [candle_to_lwc(c) for c in candles[-HISTORY_COUNT:]]
                candle_store[symbol] = converted
                print(f"[{symbol}] polled {len(converted)} candles")
                msg = json.dumps({"symbol": symbol, "history": converted})
                for ws in subscribers[:]:
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        if ws in subscribers:
                            subscribers.remove(ws)
            else:
                print(f"[{symbol}] candles_many returned empty")
    except Exception as e:
        print(f"[poll] candles_many error: {e!r}")


async def stream_forming(client: PocketOptionAsync, symbol: str):
    """Keep a lightweight stream just for the forming candle."""
    print(f"[{symbol}] starting forming-candle stream...")
    try:
        async for closed, forming in client.get_candles_live(
            symbol, period=TIMEFRAME, hours=0.1, max_rows=2
        ):
            if forming:
                lwc = candle_to_lwc(forming)
                msg = json.dumps({"symbol": symbol, "candle": lwc})
                for ws in subscribers[:]:
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        if ws in subscribers:
                            subscribers.remove(ws)
            await asyncio.sleep(0.1)
    except Exception as e:
        print(f"[{symbol}] forming stream error: {e!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID environment variable is not set")
    client = PocketOptionAsync(SSID)
    await asyncio.sleep(5)

    # Initial poll for history
    await poll_historical(client)

    # Start forming-candle streams (not for history, only for live updates)
    tasks = [asyncio.create_task(stream_forming(client, s)) for s in SYMBOLS]

    # Periodic re-poll every 30 seconds to keep history fresh
    async def repoll():
        while True:
            await asyncio.sleep(30)
            await poll_historical(client)

    tasks.append(asyncio.create_task(repoll()))
    print("[lifespan] polling + streams started")
    yield
    for t in tasks:
        t.cancel()
    print("[lifespan] shutdown")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return JSONResponse({
        "status": "ok",
        "symbols": {s: len(candle_store[s]) for s in SYMBOLS},
        "subscribers": len(subscribers)
    })


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    subscribers.append(ws)
    try:
        for symbol, candles in candle_store.items():
            await ws.send_text(json.dumps({
                "symbol": symbol,
                "history": candles[-HISTORY_COUNT:]
            }))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in subscribers:
            subscribers.remove(ws)


app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
