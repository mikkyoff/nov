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
HISTORY_COUNT = 10

candle_store = {s: [] for s in SYMBOLS}
subscribers: list[WebSocket] = []


def candle_to_lwc(candle):
    """Convert candle dict to Lightweight Charts format."""
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


async def poll_closed_candles(client: PocketOptionAsync):
    """Poll the last 10 closed candles for each asset using get_candles()."""
    for symbol in SYMBOLS:
        try:
            # get_candles(asset, period, offset) - offset is lookback seconds
            # For 15s candles, requesting 600s (10 * 15 * 4, extra buffer) 
            # should return enough history
            candles = await client.get_candles(symbol, TIMEFRAME, TIMEFRAME * HISTORY_COUNT * 4)
            if candles:
                # Take only the last HISTORY_COUNT (10) closed candles
                converted = [candle_to_lwc(c) for c in candles[-HISTORY_COUNT:]]
                candle_store[symbol] = converted
                print(f"[{symbol}] polled {len(converted)} closed candles")
                # Broadcast to WebSocket clients
                msg = json.dumps({"symbol": symbol, "history": converted})
                for ws in subscribers[:]:
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        if ws in subscribers:
                            subscribers.remove(ws)
            else:
                print(f"[{symbol}] get_candles returned empty")
        except Exception as e:
            print(f"[{symbol}] poll error: {e!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID environment variable is not set")
    client = PocketOptionAsync(SSID)
    await asyncio.sleep(5)

    # Initial poll
    await poll_closed_candles(client)

    # Optional: re-poll every 30 seconds to keep the 10 candles fresh
    async def repoll():
        while True:
            await asyncio.sleep(30)
            await poll_closed_candles(client)

    task = asyncio.create_task(repoll())
    print("[lifespan] polling started")
    yield
    task.cancel()
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
        # Send whatever history is already in memory
        for symbol, candles in candle_store.items():
            await ws.send_text(json.dumps({
                "symbol": symbol,
                "history": candles
            }))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in subscribers:
            subscribers.remove(ws)


app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
