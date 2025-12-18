import pandas as pd
from datetime import date
from sqlalchemy.orm import Session

from ..models.option_quotes import OptionQuote

def load_option_data(
        session: Session,
        symbol: str,
        trade_date: date,
        term_groups: tuple[str, str],
):
    near_group, next_group = term_groups

    rows = (
        session.query(OptionQuote)
        .filter(
            OptionQuote.symbol == symbol,
            OptionQuote.trade_date == trade_date,
            OptionQuote.term_group.in_(term_groups),
            OptionQuote.cp.in_(['C', 'P']),
        ).all()
    )

    df = pd.DataFrame([r.__dict__ for r in rows])
    df = df.drop(columns=["_sa_instance_state"])

    call_near = df[(df.term_group == near_group) & (df.cp == "C")].copy()
    put_near  = df[(df.term_group == near_group) & (df.cp == "P")].copy()
    call_next = df[(df.term_group == next_group) & (df.cp == "C")].copy()
    put_next  = df[(df.term_group == next_group) & (df.cp == "P")].copy()

    return call_near, put_near, call_next, put_next