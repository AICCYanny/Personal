from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Index
from sqlalchemy.sql import func
from .base import Base

class VIXIndexValues(Base):
    __tablename__ = 'vix_index_values'

    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False)
    trade_date = Column(Date, nullable=False)

    index_type = Column(String, nullable=False)
    vix_value = Column(Float, nullable=False)

    variance_near = Column(Float)
    variance_next = Column(Float)
    t_near = Column(Float)
    t_next = Column(Float)

    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index(
            'ix_vix_symbol_date', 
            'symbol', 
            'trade_date', 
            'index_type', 
            unique=True
        ),
    )