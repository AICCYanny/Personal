import numpy as np
import pandas as pd
from scipy.interpolate import CubicSpline
from datetime import date

CMT_TENOR_DAYS = {
    "1M": 30,
    "2M": 60,
    "3M": 91,
    "6M": 182,
    "1Y": 365,
    "2Y": 730,
    "3Y": 1095,
    "5Y": 1825,
    "7Y": 2555,
    "10Y": 3650,
    "20Y": 7300,
    "30Y": 10950,
}

def bounded_cubic_spline(x, y):
    spline = CubicSpline(x, y, bc_type="natural")

    def interp(t):
        val = float(spline(t))

        if t <= x[0]:
            return y[0]
        if t >= x[-1]:
            return y[-1]

        i = np.searchsorted(x, t) - 1
        lower, upper = y[i], y[i + 1]

        return min(max(val, min(lower, upper)), max(lower, upper))

    return interp

def bey_to_cc_rate(bey):
    apy = (1 + bey / 200) ** 2 - 1
    return np.log(1 + apy)

