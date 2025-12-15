from datetime import datetime, timedelta
import pandas as pd
from .fetch_snapshot import fetch_option_snapshot

def calculate_friday_differences(given_date):
    # Calculate the last Friday in the future that is 23 to 30 days away
    last_friday_future = None
    for i in range(24, 31):  # Check from 23 to 30 days in the future
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

    while True:
        sid = fetch_option_snapshot(symbol, trade_date, 'P', near_dte, near_dte)
        if sid is None:
            near_dte -= 1
            continue
        break
    if sid == -1:
        return {"near": "skip"}
    else:
        results["near_P"] = sid
        results["near_C"] = fetch_option_snapshot(symbol, trade_date, 'C', near_dte, near_dte)

    while True:
        sid = fetch_option_snapshot(symbol, trade_date, 'P', next_dte, next_dte)
        if sid is None:
            next_dte -= 1
            continue
        break
    if sid == -1:
        return {"next": "skip"}
    else:
        results["next_P"] = sid
        results["next_C"] = fetch_option_snapshot(symbol, trade_date, 'C', next_dte, next_dte)

    return results
