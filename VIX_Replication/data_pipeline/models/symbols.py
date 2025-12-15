from sqlalchemy import Column, Integer, Text
from .base import Base

class Symbol(Base):
    __tablename__ = 'symbols'

    id = Column(Integer, primary_key=True)
    symbol = Column(Text, unique=True, nullable=False)
    description = Column(Text)

    