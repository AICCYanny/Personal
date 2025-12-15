from sqlalchemy import func

from .engine import SessionLocal
from ..models.option_quotes import OptionQuote
from ..models.symbols import Symbol

def get_symbol_record(symbol: str):
    with SessionLocal() as s:
        return s.query(Symbol).filter(Symbol.symbol == symbol).first()
    
def create_symbol_if_not_exists(symbol: str, description=None):
    with SessionLocal() as s:
        rec = s.query(Symbol).filter_by(symbol=symbol).first()
        if rec:
            return rec

        rec = Symbol(
            symbol=symbol,
            description=description,
            first_option_date=None,
            last_option_date=None,
            is_active=True
        )
        s.add(rec)
        s.commit()
        s.refresh(rec)
        return rec