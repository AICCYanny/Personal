from sqlalchemy import Column, Integer, String, Float, Date, Index
from .base import Base

class RiskFreeRate(Base):
    __tablename__ = 'risk_free_rates'

    id = Column(Integer, primary_key=True)

    trade_date = Column(Date, nullable=False)
    tenor = Column(String, nullable=False)
    rate_bey = Column(Float, nullable=False)

    __table_args__ = (
        Index('ix_rate_date_tenor', 'trade_date', 'tenor', unique=True),
    )