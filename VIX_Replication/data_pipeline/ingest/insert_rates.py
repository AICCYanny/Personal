import pandas as pd
import pandas_datareader.data as web
from datetime import datetime
from typing import Optional

from ..db.engine import SessionLocal
from ..models.rates import RiskFreeRate

FRED_TO_TENOR = {
    "DGS1MO": "1M",
    "DGS3MO": "3M",
    "DGS6MO": "6M",
    "DGS1": "1Y",
    "DGS2": "2Y",
    "DGS3": "3Y",
    "DGS5": "5Y",
    "DGS7": "7Y",
    "DGS10": "10Y",
    "DGS20": "20Y",
    "DGS30": "30Y",
}

def ingest_fred_rates(
    start_date: str = "2000-01-01",
    end_date: Optional[str] = None,
    overwrite: bool = False,
):
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")

    print(f"[INGEST] FRED rates {start_date} → {end_date}")

    fred_codes = list(FRED_TO_TENOR.keys())

    rates_df = web.DataReader(
        fred_codes,
        "fred",
        start=start_date,
        end=end_date,
    )

    session = SessionLocal()
    inserted = 0
    skipped = 0

    try:
        for trade_date, row in rates_df.iterrows():
            trade_date = trade_date.date()

            for fred_code, tenor in FRED_TO_TENOR.items():
                val = row.get(fred_code)

                if pd.isna(val):
                    continue

                if overwrite:
                    rec = RiskFreeRate(
                        trade_date=trade_date,
                        tenor=tenor,
                        rate_bey=float(val),
                    )
                    session.merge(rec)
                    inserted += 1
                else:
                    exists = (
                        session.query(RiskFreeRate)
                        .filter(
                            RiskFreeRate.trade_date == trade_date,
                            RiskFreeRate.tenor == tenor,
                        )
                        .first()
                    )

                    if exists:
                        skipped += 1
                        continue

                    rec = RiskFreeRate(
                        trade_date=trade_date,
                        tenor=tenor,
                        rate_bey=float(val),
                    )
                    session.add(rec)
                    inserted += 1

        session.commit()

    finally:
        session.close()

    print(
        f"[DONE] Rates ingest finished | inserted={inserted}, skipped={skipped}"
    )