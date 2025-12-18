import requests
import os
from dotenv import load_dotenv
import time
import asyncio
import aiohttp
import pandas as pd
from io import BytesIO


from ..db.engine import SessionLocal
from ..db.query_helpers import exists_snapshot
from .parse_and_insert import parse_and_insert_quotes

load_dotenv()

API_URL = os.getenv('API_URL')
API_KEY = os.getenv('IVOL_API_KEY')

MAX_RETRIES = 6

def poll_download_info(detail_url, max_tries=10, delay=0.5):
    for _ in range(max_tries):
        detail_json = requests.get(detail_url).json()
        file_info = detail_json[0]["data"][0]
        url = file_info.get("urlForDownload")
        size = file_info.get("fileSize", 0)

        if url and size > 0:
            return url

        time.sleep(delay)

    raise RuntimeError("download file not ready")

async def poll_detail_until_ready(detail_url, session, timeout=180, interval=0.5, max_interval=5.0):
    """
    Polls the urlForDetails endpoint until 'urlForDownload' becomes available.
    """
    start = asyncio.get_event_loop().time()
    sleep = interval

    while True:
        async with session.get(detail_url) as resp:
            data = await resp.json()

        # usual structure:
        #  [ { "data": [ { "urlForDownload": ... } ] } ]
        try:
            file_info = data[0]["data"][0]
            download_url = file_info.get("urlForDownload")

            if download_url:
                return download_url
        except Exception:
            pass

        # timeout
        if asyncio.get_event_loop().time() - start > timeout:
            raise TimeoutError(f"Polling timed out for detail URL: {detail_url}")

        await asyncio.sleep(sleep)
        sleep = min(sleep * 1.3, max_interval)

def download_csv(download_url: str) -> pd.DataFrame:
    resp = requests.get(download_url, timeout=15)
    resp.raise_for_status()
    df = pd.read_csv(BytesIO(resp.content), compression="gzip")
    return df

async def async_download_csv(download_url: str, session: aiohttp.ClientSession) -> pd.DataFrame:
    async with session.get(download_url, timeout=15) as resp:
        resp.raise_for_status()
        content = await resp.read()
        df = pd.read_csv(BytesIO(content), compression="gzip")
        return df

def safe_request(url, params):
    retries = 0

    while retries <= MAX_RETRIES:
        try:
            resp = requests.get(url, params=params, timeout=10)

            if resp.status_code == 429:
                wait = 2 ** retries
                print(f'[429] sleep {wait}s')
                time.sleep(wait)
                retries += 1
                continue

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            wait = 2 ** retries
            print(f'[ERROR] {e}, sleep {wait}s')
            time.sleep(wait)
            retries += 1

    print('[ERROR] max retries exceeded')
    return None

def fetch_option_snapshot(symbol: str, trade_date: str, cp: str):
    if exists_snapshot(
        symbol=symbol,
        trade_date=trade_date,
        cp=cp
    ):
        print(f'[Skip] snapshot exists for {symbol} {trade_date} {cp}')
        return -1

    params = {
        'symbol': symbol,
        'tradeDate': trade_date,
        'dteFrom': 0,
        'dteTo': 150,
        'cp': cp,
        "deltaFrom": -100,
        "deltaTo": 100,
        'apiKey': API_KEY
    }
    data = safe_request(API_URL, params=params)

    if data is None:
        print(f"[ERROR] No data fetched for {symbol} {trade_date} {cp}")
        return None
    
    rows = data.get('data', [])
    if len(rows) > 0:
        df = pd.DataFrame(rows)
    else:
        detail_url = data["status"].get("urlForDetails")
        if not detail_url:
            print(f"[ERROR] no data & no urlForDetails for {symbol} {trade_date} {cp}")
            return None

        download_url = poll_download_info(detail_url)
        print(download_url)

        df = download_csv(download_url)

    with SessionLocal() as session:

        inserted = parse_and_insert_quotes(
            session=session, 
            symbol=symbol,
            trade_date=trade_date,
            cp=cp,
            df=df,
        )

        print(f"[INFO] inserted {inserted} rows for {symbol} {trade_date} {cp}")
        return inserted

sem = asyncio.Semaphore(5)

async def async_safe_request(url, params, session):
    retries = 0
    while retries <= MAX_RETRIES:
        try:
            async with session.get(url, params=params, timeout=10) as resp:

                if resp.status == 429:
                    wait = 2 ** retries
                    print(f'[429] sleep {wait}s')
                    await asyncio.sleep(wait)
                    retries += 1
                    continue

                resp.raise_for_status()
                return await resp.json()
            
        except Exception as e:
            wait = 2 ** retries
            print(f'[NETWORK] {e}, sleep {wait}s')
            await asyncio.sleep(wait)
            retries += 1

    print('[ERROR] async max retries exceeded')
    return None

async def async_fetch_option_snapshot(symbol: str, trade_date: str, cp: str, session):
    if exists_snapshot(
        symbol=symbol,
        trade_date=trade_date,
        cp=cp
    ):
        print(f'[Skip] snapshot exists for {symbol} {trade_date} {cp}')
        return -1

    async with sem:
        params = {
            'symbol': symbol,
            'tradeDate': trade_date,
            'dteFrom': 0,
            'dteTo': 150,
            'cp': cp,
            "deltaFrom": -100,
            "deltaTo": 100,
            'apiKey': API_KEY
        }

        data = await async_safe_request(API_URL, params, session)

        if data is None:
            return None
        
        rows = data.get("data", [])
        if len(rows) > 0:
            df = pd.DataFrame(rows)
        else:
            detail_url = data["status"].get("urlForDetails")
            if not detail_url:
                print(f"[ERROR] no data & no urlForDetails for {symbol} {trade_date} {cp}")
                return None

            download_url = await poll_detail_until_ready(detail_url, session)
            df = await async_download_csv(download_url, session)
        
        with SessionLocal() as s:
            inserted = parse_and_insert_quotes(
                session=s,
                symbol=symbol,
                trade_date=trade_date,
                cp=cp,
                df=df
            )

        print(f"[INFO] inserted {inserted} rows for {symbol} {trade_date} {cp}")
        return inserted