from sqlalchemy import Column, Integer, String, Date, DateTime, Boolean, UniqueConstraint, func
from .base import Base

class DailySnapshot(Base):
    __tablename__ = 'daily_snapshots'

    id = Column(Integer, primary_key=True)

    symbol = Column(String, nullable=False)
    trade_date = Column(Date, nullable=False)

    completed = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint('symbol', 'trade_date', name='uq_daily_snapshot'),
    )
