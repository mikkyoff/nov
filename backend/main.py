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
REPOLL_SECONDS = 20    # re-poll cadence

candle_store = {s: [] for s in SYMBOLS}
subscribers: list[WebSocket] = []


def to_epoch(ts) -> int:
    """Convert pandas Timestamp or ISO/epoch value to integer epoch seconds."""
    if isinstance(ts, pd.Timestamp):
        return int(ts.timestamp())
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except ValueError:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
    return int(ts)


async def poll_closed_candles(client: PocketOptionAsync):
    """Poll ticks via get_candles, resample to PERIOD, keep last N closed candles."""
    for symbol in SYMBOLS:
        try:
            lookback_seconds = N_CLOSED * PERIOD * 6  # generous buffer
            rows = await client.get_candles(symbol, PERIOD, lookback_seconds)
            if not rows:
                print(f"[{symbol}] get_candles returned nothing")
                continue

            df = pd.DataFrame(rows)
            df.columns = [c.strip().lower() for c in df.columns]
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df = df.set_index("time").sort_index()
            df["price"] = df["price"].astype(float)

            rule = f"{PERIOD}s"
            ohlc = df["price"].resample(rule, label="left", closed="left").agg(
                open="first", high="max", low="min", close="last", count="count"
            )

            # Drop the forming bucket (its end is in the future relative to now)
            now = pd.Timestamp.now(tz="UTC")
            bucket_end = ohlc.index + pd.Timedelta(seconds=PERIOD)
            ohlc = ohlc[bucket_end <= now]

            # Drop sparse buckets
            sparse = ohlc[ohlc["count"] < MIN_TICKS]
            if not sparse.empty:
                print(f"[{symbol}] dropping {len(sparse)} sparse buckets (<{MIN_TICKS} ticks)")
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
            print(f"[{symbol}] {len(converted)} closed candles "
                  f"(epoch {converted[0]['time'] if converted else '-'} "
                  f"→ {converted[-1]['time'] if converted else '-'})")

            msg = json.dumps({"symbol": symbol, "history": converted})
            for ws in subscribers[:]:
                try:
                    await ws.send_text(msg)
                except Exception:
                    if ws in subscribers:
                        subscribers.remove(ws)

        except Exception as e:
            print(f"[{symbol}] poll error: {e!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID environment variable is not set")
    client = PocketOptionAsync(SSID)
    await asyncio.sleep(5)

    await poll_closed_candles(client)

    async def repoll():
        while True:
            await asyncio.sleep(REPOLL_SECONDS)
            await poll_closed_candles(client)

    task = asyncio.create_task(repoll())
    print("[lifespan] polling loop started")
    yield
    task.cancel()
    print("[lifespan] shutdown")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    detail = {}
    for s in SYMBOLS:
        c = candle_store[s]
        detail[s] = {
            "count": len(c),
            "first_epoch": c[0]["time"] if c else None,
            "last_epoch": c[-1]["time"] if c else None,
            "last_close": c[-1]["close"] if c else None,
        }
    return JSONResponse({
        "status": "ok",
        "symbols": detail,
        "subscribers": len(subscribers),
    })


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    subscribers.append(ws)
    try:
        for symbol, candles in candle_store.items():
            await ws.send_text(json.dumps({"symbol": symbol, "history": candles}))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in subscribers:
            subscribers.remove(ws)


app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
