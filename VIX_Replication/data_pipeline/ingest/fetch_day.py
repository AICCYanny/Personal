from datetime import datetime, timedelta
import pandas as pd

from .fetch_snapshot import fetch_option_snapshot, async_fetch_option_snapshot
from ..db.engine import SessionLocal
from ..models.symbols import Symbol

def calculate_friday_differences(given_date):
    # Calculate the last Friday in the future that is 23 to 30 days away
    last_friday_future = None
    for i in range(29, 22, -1):  # Check from 23 to 30 days in the future
        check_date = given_date + timedelta(days=i)
        if check_date.weekday() == 4:  # 4 represents Friday
            last_friday_future = check_date
            break

    # Calculate the first Friday outside 30 days from the given date
    first_friday_outside_30_days = None
    for i in range(31, 38):  # Check from 31 to 37 days in the future
        check_date = given_date + timedelta(days=i)
        if check_date.weekday() == 4:  # 4 represents Friday
            first_friday_outside_30_days = check_date
            break

    # Calculate the differences in days
    days_to_last_friday_future = (last_friday_future - given_date).days
    days_to_first_friday_outside_30 = (first_friday_outside_30_days - given_date).days

    return days_to_last_friday_future, days_to_first_friday_outside_30

def fetch_four_snapshots(symbol: str, trade_date: str):
    t_date = datetime.strptime(trade_date, "%Y-%m-%d")
    near_dte, next_dte = calculate_friday_differences(t_date)

    results = {}
    MAX_DTE_BACKTRACK = 10
    attempts_near = 0
    attempts_next = 0

    while True:
        sid = fetch_option_snapshot(symbol, trade_date, 'P', near_dte, near_dte)
        if sid == -1:
            print(f"[SKIP] existing near-term snapshot for {symbol} {trade_date}")
            return {"near": "skip"}
        if sid is None:
            near_dte -= 1
            attempts_near += 1

            if attempts_near >= MAX_DTE_BACKTRACK:
                print(f'[SKIP] {symbol} {trade_date} no near_term options (likely not listed yet)')
                return {'near': 'skip'}
            continue
        break
    results["near_P"] = sid
    results["near_C"] = fetch_option_snapshot(symbol, trade_date, 'C', near_dte, near_dte)

    while True:
        sid = fetch_option_snapshot(symbol, trade_date, 'P', next_dte, next_dte)
        if sid == -1:
            print(f"[SKIP] existing next-term snapshot for {symbol} {trade_date}")
            return {'next': 'skip'}
        if sid is None:
            next_dte -= 1
            attempts_next += 1

            if attempts_next >= MAX_DTE_BACKTRACK:
                print(f'[SKIP] {symbol} {trade_date} no next_term options (likely not listed yet)')
                return {'next': 'skip'}
            continue
        break
    results["next_P"] = sid
    results["next_C"] = fetch_option_snapshot(symbol, trade_date, 'C', next_dte, next_dte)

    with SessionLocal() as s:
        symbol_rec = s.query(Symbol).filter_by(symbol=symbol).first()
        if symbol_rec.first_option_date is None:
            symbol_rec.first_option_date = t_date
            s.commit()

    return results

async def async_fetch_four_snapshots(symbol: str, trade_date: str, session):
    t_date = datetime.strptime(trade_date, "%Y-%m-%d")
    near_dte, next_dte = calculate_friday_differences(t_date)

    MAX_BACKTRACK = 10
    attempts_near = 0
    attempts_next = 0

    while True:
        sid = await async_fetch_option_snapshot(symbol, trade_date, "P", near_dte, near_dte, session)
        if sid == -1:
            # print(f"[SKIP] existing near-term snapshot for {symbol} {trade_date}")
            return {'near': 'skip'}
        if sid is None:
            near_dte -= 1
            attempts_near += 1

            if attempts_near >= MAX_BACKTRACK:
                # print(f'[SKIP] {symbol} {trade_date} no near-term options (likely not listed yet)')
                return {'near': 'skip'}
            continue
        break

    near_P = sid
    near_C = await async_fetch_option_snapshot(symbol, trade_date, "C", near_dte, near_dte, session)

    while True:
        sid = await async_fetch_option_snapshot(symbol, trade_date, "P", next_dte, next_dte, session)
        if sid == -1:
            # print(f"[SKIP] existing next-term snapshot for {symbol} {trade_date}")
            return {'next': 'skip'}
        if sid is None:
            next_dte -= 1
            attempts_next += 1

            if attempts_next >= MAX_BACKTRACK:
                # print(f'[SKIP] {symbol} {trade_date} no next-term options (likely not listed yet)')
                return {'next': 'skip'}
            continue
        break

    next_P = sid
    next_C = await async_fetch_option_snapshot(symbol, trade_date, "C", next_dte, next_dte, session)

    with SessionLocal() as s:
        symbol_rec = s.query(Symbol).filter_by(symbol=symbol).first()
        if symbol_rec.first_option_date is None:
            symbol_rec.first_option_date = t_date
            s.commit()

    return {
        'near_P': near_P,
        'near_C': near_C,
        'next_P': next_P,
        'next_C': next_C,
    }