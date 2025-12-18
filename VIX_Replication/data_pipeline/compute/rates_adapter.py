import numpy as np
import pandas as pd
from datetime import timedelta

from ..models.rates import RiskFreeRate
from ..utils.rate_math import CMT_TENOR_DAYS, bey_to_cc_rate, bounded_cubic_spline

MAX_LOOKBACK_DAYS = 5

def load_rate_curve(session, trade_date):
    cur_date = trade_date

    for _ in range(MAX_LOOKBACK_DAYS):
        rows = (
            session.query(RiskFreeRate)
            .filter(RiskFreeRate.trade_date == cur_date)
            .all()
        )

        if rows:
            series = {r.tenor: r.rate_bey for r in rows}
            return pd.Series(series)
        
        cur_date -= timedelta(days=1)

    raise ValueError(
        f"No risk-free rate data found within {MAX_LOOKBACK_DAYS} days "
        f"before {trade_date}"
    )

def compute_r_for_expiry(dte, rates_series):
    curve = rates_series.dropna()

    df = (
        curve
        .rename("rate")
        .to_frame()
        .assign(days=lambda x: x.index.map(CMT_TENOR_DAYS))
        .dropna(subset=["days"])
        .drop_duplicates(subset=["days"])
        .sort_values("days")
    )

    x = df["days"].values.astype(float)
    y = df["rate"].values.astype(float)

    interp = bounded_cubic_spline(x, y)
    bey_t = interp(dte)

    return bey_to_cc_rate(bey_t)

def compute_r1_r2(session, trade_date, dte1, dte2):
    rates_series = load_rate_curve(session, trade_date)

    r1 = compute_r_for_expiry(dte1, rates_series)
    r2 = compute_r_for_expiry(dte2, rates_series)

    return float(r1), float(r2)