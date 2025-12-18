from .engine import engine
from ..models.vix_index_values import VIXIndexValues

if __name__ == '__main__':
    VIXIndexValues.__table__.create(bind=engine, checkfirst=True)
    print("vix_index_values table created (if not exists)")