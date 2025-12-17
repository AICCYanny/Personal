from datetime import datetime
from sqlalchemy.orm import Session
import pandas as pd

from ..models.option_quotes import OptionQuote

def parse_and_insert_quotes(
        session: Session, 
        symbol: str,
        trade_date: str,
        cp: str,
        df: pd.DataFrame
    ):
    if df is None or df.empty:
        return 0
    
    trade_date_obj = datetime.strptime(trade_date, '%Y-%m-%d').date()

    df["expiration_date"] = pd.to_datetime(df["expiration_date"])

    all_dtes = sorted(set(df["dte"][df["dte"] > 0]))

    def classify(dte):
        if not all_dtes:
            return 'other'

        below30 = [x for x in all_dtes if x < 30]
        above30 = [x for x in all_dtes if x > 30]
        near30 = max(below30) if below30 else None
        next30 = min(above30) if above30 else None

        below90 = [x for x in all_dtes if x < 90]
        above90 = [x for x in all_dtes if x > 90]
        near90 = max(below90) if below90 else None
        next90 = min(above90) if above90 else None

        if dte == near30: return 'near30'
        if dte == next30: return 'next30'
        if dte == near90: return 'near90'
        if dte == next90: return 'next90'
        return 'other'

    df["term_group"] = df["dte"].apply(classify)
    df = df[df["term_group"] != "other"].copy()

    if df.empty:
        return 0
    
    df["option_root"] = df["option_symbol"].str.extract(r'^([A-Z]+)')
    
    inserted = 0

    for _, row in df.iterrows():
        bid = row["Bid"]
        ask = row["Ask"]
        mid = (bid + ask) / 2 if bid is not None and ask is not None else row["price"]

        try:
            session.add(
                OptionQuote(
                    symbol=symbol,
                    trade_date=trade_date_obj,
                    option_symbol=row["option_symbol"],
                    option_root=row["option_root"],

                    cp=row['call_put'],
                    expiry=row["expiration_date"],
                    dte=row['dte'],
                    term_group=row['term_group'],
                    strike=row['price_strike'],
                    
                    bid=bid,
                    ask=ask,
                    mid=mid,

                    iv=row['iv'],
                    delta=row['delta'],
                    gamma=row['gamma'],
                    vega=row['vega'],
                    theta=row['theta'],

                    volume=row['volume'],
                    open_interest=row['openinterest'],
                )
            )
            
            inserted += 1
            
        except Exception as e:
           print(f'[WARNING] skip item: {e}')

    session.commit()
    return inserted