from datetime import timedelta, date

from ..utils.calendar import TRADE_DATES
from ..db.query_helpers import last_trade_date
from .. ingest.fetch_day import fetch_four_snapshots

def update_symbol(symbol: str, start_date: date):
    d = last_trade_date(symbol=symbol)

    if d is None:
        d = start_date

    else:
        d = d + timedelta(days=1)

    while True:
        if d > date.today():
            break

        if d not in TRADE_DATES:
            d = d + timedelta(days=1)
            continue

        result = fetch_four_snapshots(symbol, d.strftime('%Y-%m-%d'))

        if result is None or result == {}:
            print('No more data, stop.')
            break

        print(f'[OK] {symbol} {d}')
        d = d + timedelta(days=1)

def update_all(symbols: list, start_date: date):
    for s in symbols:
        update_symbol(s, start_date)