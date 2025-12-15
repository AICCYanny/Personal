import pandas_market_calendars as mcal

nyse = mcal.get_calendar('NYSE')
schedule = nyse.schedule('2009-01-01', '2035-12-31')

TRADE_DATES = set(schedule.index.date)