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


async def stream_symbol(client: PocketOptionAsync, symbol: str):
    """Use get_candles_live to receive both closed history and the forming candle."""
    print(f"[{symbol}] starting get_candles_live stream...")
    try:
        async for closed_candles, forming_candle in client.get_candles_live(
            symbol, period=TIMEFRAME, hours=1.0, max_rows=HISTORY_COUNT
        ):
            # Update historical closed candles (keep last HISTORY_COUNT)
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

            # Broadcast the forming candle (live update)
            if forming_candle:
                lwc = candle_to_lwc(forming_candle)
                msg = json.dumps({"symbol": symbol, "candle": lwc})
                for ws in subscribers[:]:
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        if ws in subscribers:
                            subscribers.remove(ws)

            await asyncio.sleep(0.05)

    except Exception as e:
        print(f"[{symbol}] stream error: {e!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID environment variable is not set")
    client = PocketOptionAsync(SSID)
    await asyncio.sleep(5)

    tasks = [asyncio.create_task(stream_symbol(client, s)) for s in SYMBOLS]
    print("[lifespan] stream tasks started")
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
