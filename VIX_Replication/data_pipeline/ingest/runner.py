import argparse
import asyncio
from datetime import datetime, date
from tqdm.auto import tqdm

from .update import async_update_symbol
from ..utils.calendar import TRADE_DATES


def count_days(start_date: date):
    today = date.today()
    return len([d for d in TRADE_DATES if start_date <= d <= today])


async def run_ingestion(symbols, start_date):

    symbol_bar = tqdm(
        total=len(symbols),
        desc="Symbols",
        position=0,
        leave=True,
        colour="cyan"
    )

    for sym in symbols:

        total_days = count_days(start_date)

        day_bar = tqdm(
            total=total_days,
            desc=f"{sym} days",
            position=1,
            leave=True,
            colour="green",
        )

        def day_callback():
            day_bar.update(1)

        await async_update_symbol(sym, start_date, day_callback)

        day_bar.close()
        symbol_bar.update()
    
    symbol_bar.close()
    print("🎉 All ingestion completed.")


def parse_date(d):
    return datetime.strptime(d, "%Y-%m-%d").date()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", "-s", nargs="+", required=True)
    parser.add_argument("--start", "-t", required=True)

    args = parser.parse_args()
    start_date = parse_date(args.start)

    asyncio.run(run_ingestion(args.symbols, start_date))
