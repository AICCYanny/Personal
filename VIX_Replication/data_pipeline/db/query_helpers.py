from sqlalchemy import func

from .engine import SessionLocal
from ..models.option_quotes import OptionQuote

def last_trade_date(symbol: str):
    with SessionLocal() as s:
        row = s.query(func.max(OptionQuote.trade_date)).filter_by(
            symbol=symbol
        ).first()

        return row[0]