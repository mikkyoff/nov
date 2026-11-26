import asyncio
import subprocess
import httpx
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync

SSID = os.environ.get("POCKET_OPTION_SSID")
SYMBOLS = ["EURHUF_otc", "EURUSD_otc"]
PERIOD = 15            # 15-second candles
N_CLOSED = 10          # last N closed candles to keep

# Store the closed candles we've seen
candle_store = {s: [] for s in SYMBOLS}
subscribers: list[WebSocket] = []


def to_epoch(ts) -> int:
    """Convert various timestamp formats to integer epoch seconds."""
    if isinstance(ts, (int, float)):
        return int(ts)
    if isinstance(ts, str):
        try:
            # Try ISO format first
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except ValueError:
            # Fallback for 'YYYY-MM-DD HH:MM:SS' format
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
    return int(ts)


async def stream_aligned_candles(client: PocketOptionAsync, symbol: str):
    """Use subscribe_symbol_timed to get perfectly time-aligned candles."""
    print(f"[{symbol}] starting time-aligned stream...")
    try:
        # subscribe_symbol_timed aligns the candle boundaries to exact time marks.
        stream = await client.subscribe_symbol_timed(symbol, timedelta(seconds=PERIOD))
        
        async for candle in stream:
            # The library emits closed candles. We map it to LWC format.
            lwc_candle = {
                "time": to_epoch(candle["time"]),
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
            }

            store = candle_store[symbol]
            
            # If we get a candle with the same timestamp as the last one, update it.
            # Otherwise, append the new closed candle.
            if store and store[-1]["time"] == lwc_candle["time"]:
                store[-1] = lwc_candle
            else:
                store.append(lwc_candle)
                if len(store) > N_CLOSED:
                    store.pop(0)

            # Only broadcast when we have at least 1 candle, but ideally we send the whole history
            if store:
                msg = json.dumps({"symbol": symbol, "history": store})
                for ws in subscribers[:]:
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        if ws in subscribers:
                            subscribers.remove(ws)
                        
    except Exception as e:
        print(f"[{symbol}] stream error: {e!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID environment variable is not set")
    
    client = PocketOptionAsync(SSID)
    # Give the client time to connect and sync server time
    await asyncio.sleep(5)

    tasks = [asyncio.create_task(stream_aligned_candles(client, s)) for s in SYMBOLS]
    print("[lifespan] time-aligned streams started")
    
    yield
    
    for t in tasks:
        t.cancel()
    print("[lifespan] shutdown")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    detail = {}
    for s in SYMBOLS:
        c = candle_store[s]
        detail[s] = {
            "count": len(c),
            "last_epoch": c[-1]["time"] if c else None,
            "last_close": c[-1]["close"] if c else None,
        }
    return JSONResponse({"status": "ok", "symbols": detail})


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    subscribers.append(ws)
    try:
        # Send current history immediately to the new client
        for symbol, candles in candle_store.items():
            if candles:
                await ws.send_text(json.dumps({"symbol": symbol, "history": candles}))
        # Keep connection open
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in subscribers:
            subscribers.remove(ws)

import subprocess
import httpx

@app.get("/ip-info")
async def ip_info():
    """Checks Railway's outbound IP and ISP."""
    try:
        async with httpx.AsyncClient() as client_http:
            resp = await client_http.get("https://ipinfo.io/json", timeout=10.0)
            return JSONResponse(content=resp.json())
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/speedtest")
async def run_speedtest():
    """Runs a network speed test from inside the Railway container."""
    try:
        result = subprocess.run(
            ["speedtest-cli", "--json"], 
            capture_output=True, 
            text=True, 
            timeout=60
        )
        if result.returncode != 0:
            return JSONResponse(content={"error": "Speedtest failed", "details": result.stderr}, status_code=500)
        
        data = json.loads(result.stdout)
        return JSONResponse(content={
            "download_mbps": round(data["download"] / 1_000_000, 2),
            "upload_mbps": round(data["upload"] / 1_000_000, 2),
            "ping_ms": data["ping"],
            "isp": data["client"].get("isp", "Unknown"),
            "ip": data["client"].get("ip", "Unknown")
        })
    except subprocess.TimeoutExpired:
        return JSONResponse(content={"error": "Speedtest timed out"}, status_code=504)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
