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
TIMEFRAME = 15          # 15-second period
HISTORY_COUNT = 99      # backfill 99 historical candles
MAX_KEEP = 500          # cap candles kept in memory per symbol

# In-memory candle store: { symbol: [candle_dict, ...] }
candle_store = {s: [] for s in SYMBOLS}
subscribers: list[WebSocket] = []


def candle_to_lwc(candle):
    """Convert a candle dict from the library into Lightweight Charts format."""
    t = candle.get("time")
    if isinstance(t, str):
        try:
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
            epoch = int(dt.timestamp())
        except ValueError:
            # Handles "2024-01-01 10:00:00" style timestamps
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


async def stream_symbol(client: PocketOptionAsync, symbol: str):
    """Use get_candles_live to receive both closed history and the forming candle."""
    print(f"[{symbol}] starting data stream...")
    try:
        async for closed_candles, forming_candle in client.get_candles_live(
            symbol, period=TIMEFRAME, hours=2.0, max_rows=HISTORY_COUNT
        ):
            # 1. Update historical closed candles (keep only the last HISTORY_COUNT)
            if closed_candles:
                converted = [candle_to_lwc(c) for c in closed_candles[-HISTORY_COUNT:]]
                candle_store[symbol] = converted
                msg = json.dumps({"symbol": symbol, "history": converted})
                for ws in subscribers[:]:
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        if ws in subscribers:
                            subscribers.remove(ws)

            # 2. Broadcast the currently forming candle (live update)
            if forming_candle:
                lwc = candle_to_lwc(forming_candle)
                msg = json.dumps({"symbol": symbol, "candle": lwc})
                for ws in subscribers[:]:
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        if ws in subscribers:
                            subscribers.remove(ws)

            # Yield to the event loop so the server stays responsive
            await asyncio.sleep(0.1)

    except Exception as e:
        print(f"[{symbol}] stream error: {e!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID environment variable is not set")
    client = PocketOptionAsync(SSID)
    # Give the client a moment to establish the connection
    await asyncio.sleep(5)
    tasks = [asyncio.create_task(stream_symbol(client, s)) for s in SYMBOLS]
    print("[lifespan] data stream tasks started")
    yield
    for t in tasks:
        t.cancel()
    # PocketOptionAsync has no explicit __aexit__; let it be garbage collected.
    print("[lifespan] shutting down")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    """Health endpoint for Railway and for manual verification."""
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
                "history": candles[-HISTORY_COUNT:]
            }))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in subscribers:
            subscribers.remove(ws)


# Mount the frontend (Dockerfile copies it to backend/frontend)
app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
