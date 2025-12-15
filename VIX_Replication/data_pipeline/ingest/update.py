from datetime import timedelta, date
import aiohttp
import asyncio
from asyncio import Queue

from ..utils.calendar import TRADE_DATES
from ..db.query_helpers import get_symbol_record, create_symbol_if_not_exists
from ..ingest.fetch_day import fetch_four_snapshots, async_fetch_four_snapshots
from ..db.engine import SessionLocal
from ..models.symbols import Symbol

event_queue = Queue()

def update_symbol(symbol: str, start_date: date):
    rec = get_symbol_record(symbol)
    if rec is None:
        rec = create_symbol_if_not_exists(symbol)

    if not rec.is_active:
        print(f'[INFO] {symbol} is inactive. Skip.')
        return 

    if rec.last_option_date:
        d =rec.last_option_date + timedelta(days=1)
    else:
        d = start_date

    while True:
        today = date.today()
        if d > today:
            print(f'[DONE] {symbol} up to date.')
            break

        if d not in TRADE_DATES:
            d += timedelta(days=1)
            continue

        result = fetch_four_snapshots(symbol, d.strftime('%Y-%m-%d'))

        if "skip" in result.values():
            d += timedelta(days=1)
            continue

        with SessionLocal() as s:
            rec_db = s.query(Symbol).filter_by(symbol=symbol).first()
            rec_db.last_option_date = d
            s.commit()

        print(f'[OK] {symbol} {d}')
        d += timedelta(days=1)

async def async_update_symbol(symbol: str, start_date: date, day_callback=None):
    rec = get_symbol_record(symbol)
    if rec is None:
        rec = create_symbol_if_not_exists(symbol)

    if not rec.is_active:
        # print(f'[INFO] {symbol} is inactive. Skip.')
        return
    
    if rec.last_option_date:
        d = rec.last_option_date + timedelta(days=1)
    else:
        d = start_date

    async with aiohttp.ClientSession() as http_sess:

        while True:
            today = date.today()
            if d > today:
                # print(f'[DONE] {symbol} up to date.')
                break

            if d not in TRADE_DATES:
                d += timedelta(days=1)
                continue

            await event_queue.put({
                "symbol": symbol,
                "date": d,
                "event": "start_day"
            })
            result = await async_fetch_four_snapshots(symbol, d.strftime("%Y-%m-%d"), http_sess)

            if "skip" in result.values():
                d += timedelta(days=1)
                continue

            with SessionLocal() as s:
                rec_db = s.query(Symbol).filter_by(symbol=symbol).first()
                rec_db.last_option_date = d
                s.commit()
            
            if day_callback:
                day_callback()

            # print(f'[OK] {symbol} {d}')
            d += timedelta(days=1)

def update_all(symbols: list, start_date: date):
    for s in symbols:
        update_symbol(s, start_date)

async def async_update_all(symbols: list, start_date: date):
    tasks = {}
    for sym in symbols:
        task = asyncio.create_task(async_update_symbol(sym, start_date))
        tasks[sym] = task

    return tasks