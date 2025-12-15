from sqlalchemy import Column, Integer, Text, String, Date, Boolean
from .base import Base

class Symbol(Base):
    __tablename__ = 'symbols'

    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=True, nullable=False)
    description = Column(Text)

    first_option_date = Column(Date)
    last_option_date = Column(Date)
    is_active = Column(Boolean, default=True)