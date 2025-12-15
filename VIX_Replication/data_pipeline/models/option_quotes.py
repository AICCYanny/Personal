from sqlalchemy import Column, Integer, Float, String, Date, DateTime, func, Index, UniqueConstraint
from .base import Base

class OptionQuote(Base):
    __tablename__ = 'option_quotes'

    id = Column(Integer, primary_key=True)

    symbol = Column(String, nullable=False)
    trade_date = Column(Date, nullable=False)

    cp = Column(String, nullable=False)
    dte_from = Column(Integer, nullable=False)
    dte_to = Column(Integer, nullable=False)

    strike = Column(Float, nullable=False)
    expiry = Column(Date, nullable=False)

    bid = Column(Float)
    ask = Column(Float)
    mid = Column(Float)

    iv = Column(Float)
    delta = Column(Float)
    gamma = Column(Float)
    vega = Column(Float)
    theta = Column(Float)

    volume = Column(Integer)
    open_interest = Column(Integer)

    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index('idx_symbol_date', 'symbol', 'trade_date'),
        UniqueConstraint(
            'symbol', 'trade_date', 'expiry', 'cp', 'strike',
            name='uq_optionquote'
        ),
    )