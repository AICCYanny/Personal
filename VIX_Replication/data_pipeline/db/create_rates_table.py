from .engine import engine
from ..models.rates import RiskFreeRate

if __name__ == "__main__":
    RiskFreeRate.__table__.create(bind=engine, checkfirst=True)
    print("risk_free_rates table created (if not exists)")