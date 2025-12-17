from datetime import datetime

from .fetch_snapshot import fetch_option_snapshot, async_fetch_option_snapshot
from ..db.engine import SessionLocal
from ..models.symbols import Symbol

def fetch_day(symbol: str, trade_date: str):
    results = {}
    results['C'] = fetch_option_snapshot(symbol, trade_date, 'C')
    results['P'] = fetch_option_snapshot(symbol, trade_date, 'P')

    with SessionLocal() as s:
        rec = s.query(Symbol).filter_by(symbol=symbol).first()
        if rec.first_option_date is None:
            rec.first_option_date = datetime.strptime(trade_date, '%Y-%m-%d')
            s.commit()

    return results

async def async_fetch_day(symbol: str, trade_date: str, session):
    results = {}
    results['C'] = await async_fetch_option_snapshot(symbol, trade_date, 'C', session)
    results['P'] = await async_fetch_option_snapshot(symbol, trade_date, 'P', session)

    with SessionLocal() as s:
        rec = s.query(Symbol).filter_by(symbol=symbol).first()
        if rec.first_option_date is None:
            rec.first_option_date = datetime.strptime(trade_date, '%Y-%m-%d')
            s.commit()

    return results
