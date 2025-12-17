from datetime import timedelta, date
import aiohttp
import asyncio

from ..utils.calendar import TRADE_DATES
from ..db.query_helpers import get_symbol_record, create_symbol_if_not_exists, snapshot_done, mark_snapshot_done
from ..ingest.fetch_day import fetch_day, async_fetch_day
from ..db.engine import SessionLocal
from ..models.symbols import Symbol

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

        if snapshot_done(symbol, d):
            print(f"[SKIP] {symbol} {d} already completed snapshot.")
            d += timedelta(days=1)
            continue

        result = fetch_day(symbol, d.strftime('%Y-%m-%d'))

        mark_snapshot_done(symbol, d)

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
        print(f'[INFO] {symbol} is inactive. Skip.')
        return
    
    if rec.last_option_date:
        d = rec.last_option_date + timedelta(days=1)
    else:
        d = start_date

    async with aiohttp.ClientSession() as http_sess:

        while True:
            today = date.today()
            if d > today:
                print(f'[DONE] {symbol} up to date.')
                break

            if d not in TRADE_DATES:
                d += timedelta(days=1)
                continue

            if snapshot_done(symbol, d):
                d += timedelta(days=1)
                continue

            await async_fetch_day(symbol, d.strftime("%Y-%m-%d"), http_sess)
            
            mark_snapshot_done(symbol, d)

            with SessionLocal() as s:
                rec_db = s.query(Symbol).filter_by(symbol=symbol).first()
                rec_db.last_option_date = d
                s.commit()
            
            if day_callback:
                day_callback()

            print(f'[OK] {symbol} {d}')
            d += timedelta(days=1)

def update_all(symbols: list, start_date: date):
    for s in symbols:
        update_symbol(s, start_date)

async def async_update_all(symbols: list, start_date: date):
    tasks = [
        asyncio.create_task(async_update_symbol(sym, start_date))
        for sym in symbols
    ]
    await asyncio.gather(*tasks)