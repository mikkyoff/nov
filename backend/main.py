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
HISTORY_COUNT = 99

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


async def fetch_historical(client: PocketOptionAsync, symbol: str):
    """Fetch history from the server endpoint (matches broker chart)."""
    try:
        # get_candles_advanced pulls from loadHistoryPeriod (server data)
        hist = await client.get_candles_advanced(
            symbol, period=TIMEFRAME, offset=HISTORY_COUNT * TIMEFRAME, time=0
        )
        if hist:
            converted = [candle_to_lwc(c) for c in hist[-HISTORY_COUNT:]]
            candle_store[symbol] = converted
            print(f"[{symbol}] backfilled {len(converted)} candles from server")
            # Broadcast history to all connected clients
            msg = json.dumps({"symbol": symbol, "history": converted})
            for ws in subscribers[:]:
                try:
                    await ws.send_text(msg)
                except Exception:
                    if ws in subscribers:
                        subscribers.remove(ws)
        else:
            print(f"[{symbol}] get_candles_advanced returned no data")
    except Exception as e:
        print(f"[{symbol}] backfill error: {e!r}")


async def stream_symbol(client: PocketOptionAsync, symbol: str):
    """Stream forming candle only; history is fetched separately."""
    print(f"[{symbol}] starting live stream...")
    try:
        async for closed_candles, forming_candle in client.get_candles_live(
            symbol, period=TIMEFRAME, hours=0.5, max_rows=5
        ):
            # Only use the forming candle for live updates; ignore compiled history
            if forming_candle:
                lwc = candle_to_lwc(forming_candle)
                msg = json.dumps({"symbol": symbol, "candle": lwc})
                for ws in subscribers[:]:
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        if ws in subscribers:
                            subscribers.remove(ws)
            await asyncio.sleep(0.1)
    except Exception as e:
        print(f"[{symbol}] stream error: {e!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID not set")
    client = PocketOptionAsync(SSID)
    await asyncio.sleep(5)

    # Fetch server history first, then start live streams
    await asyncio.gather(*[fetch_historical(client, s) for s in SYMBOLS])
    tasks = [asyncio.create_task(stream_symbol(client, s)) for s in SYMBOLS]
    print("[lifespan] tasks started")
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
            await ws.send_text(json.dumps({"symbol": symbol, "history": candles[-HISTORY_COUNT:]}))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in subscribers:
            subscribers.remove(ws)


app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
