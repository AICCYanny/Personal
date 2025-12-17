from ..models.base import Base
from .engine import engine
from ..models.symbols import Symbol
from ..models.option_quotes import OptionQuote
from ..models.daily_snapshot import DailySnapshot

def create_all():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

if __name__ == '__main__':
    create_all()
    print('Database tables created successfully.')