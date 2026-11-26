import asyncio
import json
import os
import subprocess
import httpx
import time
import socket
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from BinaryOptionsToolsV2.pocketoption import PocketOptionAsync

SSID = os.environ.get("POCKET_OPTION_SSID")
SYMBOLS = ["EURHUF_otc", "EURUSD_otc"]
PERIOD = 15            # 15-second candles
N_CLOSED = 10          # last N closed candles to keep

candle_store = {s: [] for s in SYMBOLS}
subscribers: list[WebSocket] = []
server_time_offset = 0.0 

async def sync_server_time(client: PocketOptionAsync):
    """Calculate the offset between local time and PocketOption server time."""
    global server_time_offset
    try:
        server_time = await client.get_server_time()
        
        if isinstance(server_time, (int, float)):
            server_ts = float(server_time)
        elif isinstance(server_time, datetime):
            server_ts = server_time.timestamp()
        else:
            server_ts = float(str(server_time))
        
        if server_ts > 1e11:
            server_ts /= 1000.0

        local_ts = time.time()
        offset = server_ts - local_ts
        
        if abs(offset) > 43200:
            print(f"[TimeSync] Ignoring unrealistic offset: {offset}s. Defaulting to 0.")
            server_time_offset = 0.0
        else:
            server_time_offset = offset
            print(f"[TimeSync] Offset calculated: {server_time_offset:.3f} seconds")
            
    except Exception as e:
        print(f"[TimeSync] Failed to sync server time: {e}. Defaulting to local time.")

async def stream_and_build_ticks(client: PocketOptionAsync, symbol: str):
    print(f"[{symbol}] starting raw tick stream with manual 15s alignment...")
    
    current_bucket = None
    forming_candle = None

    try:
        stream = await client.subscribe_symbol(symbol)
        
        async for tick in stream:
            now_ts = time.time() + server_time_offset
            bucket_start = int(now_ts // PERIOD) * PERIOD
            
            price = float(tick.get("close", tick.get("price", 0.0)))
            if price == 0.0:
                continue
            
            if current_bucket is None:
                current_bucket = bucket_start
                forming_candle = {
                    "time": bucket_start,
                    "open": price, "high": price, "low": price, "close": price
                }
            
            elif bucket_start > current_bucket:
                store = candle_store[symbol]
                store.append(forming_candle)
                if len(store) > N_CLOSED:
                    store.pop(0)
                
                current_bucket = bucket_start
                forming_candle = {
                    "time": bucket_start,
                    "open": price, "high": price, "low": price, "close": price
                }
                
            else:
                forming_candle["high"] = max(forming_candle["high"], price)
                forming_candle["low"] = min(forming_candle["low"], price)
                forming_candle["close"] = price

            msg = json.dumps({
                "symbol": symbol, 
                "history": candle_store[symbol],
                "forming": forming_candle
            })
            for ws in subscribers[:]:
                try:
                    await ws.send_text(msg)
                except Exception:
                    if ws in subscribers:
                        subscribers.remove(ws)
                        
            await asyncio.sleep(0.01)

    except Exception as e:
        print(f"[{symbol}] stream error: {e!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID environment variable is not set")
    
    async with PocketOptionAsync(SSID) as client:
        print("[lifespan] Connected. Waiting for assets to load...")
        
        try:
            await client.wait_for_assets()
        except Exception as e:
            print(f"[lifespan] wait_for_assets warning: {e}")
            
        await sync_server_time(client)

        tasks = [asyncio.create_task(stream_and_build_ticks(client, s)) for s in SYMBOLS]
        print("[lifespan] manual alignment streams started")
        
        yield
        
        for t in tasks:
            t.cancel()
        print("[lifespan] shutdown")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return JSONResponse({
        "status": "ok", 
        "time_offset": round(server_time_offset, 3),
        "symbols": {s: len(candle_store[s]) for s in SYMBOLS}
    })


@app.get("/ip-info")
async def ip_info():
    try:
        async with httpx.AsyncClient() as client_http:
            resp = await client_http.get("https://ipinfo.io/json", timeout=10.0)
            return JSONResponse(content=resp.json())
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/speedtest")
async def run_speedtest():
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


@app.get("/ping-pocket")
async def ping_pocket():
    """Pings PocketOption's WebSocket servers to check latency."""
    hosts = [
        "api-eu.po.market",
        "api-us-south.po.market",
        "api-msk.po.market"
    ]
    results = {}
    for host in hosts:
        try:
            start = time.time()
            sock = socket.create_connection((host, 443), timeout=5)
            end = time.time()
            sock.close()
            results[host] = round((end - start) * 1000, 2)
        except Exception as e:
            results[host] = f"Failed: {e}"
    return JSONResponse(content=results)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    subscribers.append(ws)
    try:
        for symbol in SYMBOLS:
            await ws.send_text(json.dumps({
                "symbol": symbol, 
                "history": candle_store[symbol],
                "forming": None
            }))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in subscribers:
            subscribers.remove(ws)


app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
