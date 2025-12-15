import requests
import os
from dotenv import load_dotenv
import time
import random 
from datetime import datetime 
import aiohttp
import asyncio

from ..db.engine import SessionLocal
from .parse_and_insert import parse_and_insert_quotes
from ..models.option_quotes import OptionQuote

load_dotenv()

API_URL = os.getenv('API_URL')
API_KEY = os.getenv('IVOL_API_KEY')

async def async_safe_request(url, params, session):
    MAX_RETRIES = 6
    retries = 0
    while retries <= MAX_RETRIES:
        try:
            async with session.get(url, params=params, timeout=10) as resp:
                if resp.status == 429:
                    wait = 2 ** retries
                    # print(f'[429] sleep {wait}s')
                    await asyncio.sleep(wait)
                    retries += 1
                    continue

                resp.raise_for_status()
                return await resp.json()
            
        except aiohttp.ClientError as e:
            wait = 2 ** retries
            # print(f'[NETWORK] {e}, sleep {wait}s')
            await asyncio.sleep(wait)
            retries += 1

    return None

def safe_request(url, params):
    retries = 0
    while retries < 6:
        try:
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = 2 ** retries
                print(f"Got 429. Sleep {wait}s")
                time.sleep(wait)
                retries += 1
                continue
            raise

def exists_snapshot(
        symbol: str,
        trade_date: str,
        cp: str,
        dte_from: int,
        dte_to: int,
    ):
    trade_date_obj = datetime.strptime(trade_date, "%Y-%m-%d").date()

    with SessionLocal() as s:
        return s.query(OptionQuote.id).filter_by(
            symbol=symbol,
            trade_date=trade_date_obj,
            cp=cp,
            dte_from=dte_from,
            dte_to=dte_to
        ).first() is not None
    
sem = asyncio.Semaphore(5)

async def async_fetch_option_snapshot(symbol: str, trade_date: str, cp: str, dte_from: int, dte_to: int, session):
    if exists_snapshot(
        symbol=symbol,
        trade_date=trade_date,
        cp=cp,
        dte_from=dte_from,
        dte_to=dte_to
    ):
        # print('Skip, Already Exists.')
        return -1

    async with sem:
        params = {
            'symbol': symbol,
            'tradeDate': trade_date,
            'dteFrom': dte_from,
            'dteTo': dte_to,
            'cp': cp,
            "deltaFrom": -100,
            "deltaTo": 100,
            'apiKey': API_KEY
        }

        data = await async_safe_request(API_URL, params, session)

        if not data['data']:
            return None
        
        with SessionLocal() as s:
            inserted = parse_and_insert_quotes(
                session=s,
                symbol=symbol,
                trade_date=trade_date,
                cp=cp,
                dte_from=dte_from,
                dte_to=dte_to,
                raw_json=data
            )

            # print(f"[INFO] parsed {inserted} option quotes")
            await asyncio.sleep(0.2 + random.random()*0.3)

        return inserted

def fetch_option_snapshot(symbol: str, trade_date: str, cp: str, dte_from: int, dte_to: int):
    if exists_snapshot(
        symbol=symbol,
        trade_date=trade_date,
        cp=cp,
        dte_from=dte_from,
        dte_to=dte_to
    ):
        print('Skip, Already Exists.')
        return -1

    params = {
        'symbol': symbol,
        'tradeDate': trade_date,
        'dteFrom': dte_from,
        'dteTo': dte_to,
        'cp': cp,
        "deltaFrom": -100,
        "deltaTo": 100,
        'apiKey': API_KEY
    }

    response = safe_request(API_URL, params=params)
    data = response.json()

    if not data['data']:
        return None

    with SessionLocal() as session:

        inserted = parse_and_insert_quotes(
            session=session, 
            symbol=symbol,
            trade_date=trade_date,
            cp=cp,
            dte_from=dte_from,
            dte_to=dte_to,
            raw_json=data,
        )

        print(f"[INFO] parsed {inserted} option quotes")
        time.sleep(0.2 + random.random()*0.3)

        return inserted

