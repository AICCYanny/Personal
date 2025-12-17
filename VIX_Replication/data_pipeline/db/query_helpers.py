from sqlalchemy import func
from datetime import datetime, date

from .engine import SessionLocal
from ..models.option_quotes import OptionQuote
from ..models.symbols import Symbol
from ..models.daily_snapshot import DailySnapshot

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
    
def exists_snapshot(symbol: str, trade_date: str, cp: str):
    trade_date_obj = datetime.strptime(trade_date, "%Y-%m-%d").date()

    with SessionLocal() as s:
        return s.query(OptionQuote.id).filter_by(
            symbol=symbol,
            trade_date=trade_date_obj,
            cp=cp,
        ).first() is not None
    
def snapshot_done(symbol: str, trade_date: date):
    with SessionLocal() as s:
        rec = s.query(DailySnapshot).filter_by(
            symbol=symbol,
            trade_date=trade_date
        ).first()
        return rec is not None and rec.completed
    
def mark_snapshot_done(symbol: str, trade_date: date):
    with SessionLocal() as s:
        rec = s.query(DailySnapshot).filter_by(
            symbol=symbol,
            trade_date=trade_date
        ).first()

        if rec:
            rec.completed = True
        else:
            rec = DailySnapshot(
                symbol=symbol,
                trade_date=trade_date,
                completed=True
            )
            s.add(rec)

        s.commit()