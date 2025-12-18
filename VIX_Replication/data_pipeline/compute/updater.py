from datetime import date
from sqlalchemy import text, func
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from ..db.engine import SessionLocal, engine
from ..models.option_quotes import OptionQuote
from ..models.vix_index_values import VIXIndexValues

from .loader import load_option_data
from .calculator import compute_vix_from_dataframes
from .rates_adapter import compute_r1_r2

VIX_INDEX_CONFIG = {
    "VIX": {
        "term_groups": ("near30", "next30"),
        "M_CM": 30,
    },
    "VIX3M": {
        "term_groups": ("near90", "next90"),
        "M_CM": 90,
    },
}

def clear_vix_table(symbol: str | None = None, index_type: str | None = None):
    if symbol is None and index_type is None:
        sql = "TRUNCATE TABLE vix_index_values"
        with engine.begin() as conn:
            conn.execute(text(sql))
        print("[CLEAN] vix_index_values fully truncated")
        return

    conditions = []
    params = {}

    if symbol is not None:
        conditions.append("symbol = :symbol")
        params["symbol"] = symbol

    if index_type is not None:
        conditions.append("index_type = :index_type")
        params["index_type"] = index_type

    where_clause = " AND ".join(conditions)
    sql = f"DELETE FROM vix_index_values WHERE {where_clause}"

    with engine.begin() as conn:
        conn.execute(text(sql), params)

    print(f"[CLEAN] vix_index_values deleted ({symbol=}, {index_type=})")

def upsert_vix(session, data: dict):
    stmt = insert(VIXIndexValues).values(**data)

    stmt = stmt.on_conflict_do_update(
        index_elements=["symbol", "trade_date", "index_type"],
        set_={
            "vix_value": stmt.excluded.vix_value,
            "variance_near": stmt.excluded.variance_near,
            "variance_next": stmt.excluded.variance_next,
            "t_near": stmt.excluded.t_near,
            "t_next": stmt.excluded.t_next,
        }
    )

    session.execute(stmt)

def run_single_day_vix(
        symbol: str, 
        trade_date: date, 
        index_type: str,
):
    if index_type not in VIX_INDEX_CONFIG:
        raise ValueError(f"Unknown index_type: {index_type}")
    
    cfg = VIX_INDEX_CONFIG[index_type]

    session: Session = SessionLocal()

    try:
        call_near, put_near, call_next, put_next = load_option_data(
            session=session, 
            symbol=symbol, 
            trade_date=trade_date,
            term_groups=cfg['term_groups']
        )

        if call_near.empty or call_next.empty:
            print(f"[SKIP] {symbol} {trade_date} (missing option data)")
            return
        
        dte1 = call_near['dte'].iloc[0]
        dte2 = call_next['dte'].iloc[0]

        r1, r2 = compute_r1_r2(
            session=session,
            trade_date=trade_date,
            dte1=dte1,
            dte2=dte2,
        )

        result = compute_vix_from_dataframes(
            call_near=call_near,
            put_near=put_near,
            call_next=call_next,
            put_next=put_next,
            trade_date=trade_date,
            r1=r1,
            r2=r2,
            M_CM=cfg['M_CM'],
        )

        data = dict(
            symbol=symbol,
            trade_date=trade_date,
            index_type=index_type,
            vix_value=result["vix"],
            variance_near=result["variance_near"],
            variance_next=result["variance_next"],
            t_near=result["t_near"],
            t_next=result["t_next"],
        )

        upsert_vix(session, data)
        session.commit()

        print(f"[OK] {symbol} {trade_date}  VIX={result['vix']:.3f}")

    finally:
        session.close()

def get_missing_vix_dates(session, symbol, index_type):
    subq = (
        session.query(VIXIndexValues.trade_date)
        .filter(
            VIXIndexValues.symbol == symbol,
            VIXIndexValues.index_type == index_type,
        )
        .subquery()
    )

    rows = (
        session.query(func.distinct(OptionQuote.trade_date))
        .filter(
            OptionQuote.symbol == symbol,
            ~OptionQuote.trade_date.in_(subq)
        )
        .order_by(OptionQuote.trade_date)
        .all()
    )

    return [r[0] for r in rows]

def run_vix_history(
    symbol: str,
    index_type: str,
    start_date: date | None = None,
    end_date: date | None = None,
    clear_existing: bool = False,
):
    if index_type not in VIX_INDEX_CONFIG:
        raise ValueError(f"Unknown index_type: {index_type}")
    
    if clear_existing:
        clear_vix_table(symbol=symbol, index_type=index_type)

    session: Session = SessionLocal()

    try:
        dates = get_missing_vix_dates(session, symbol, index_type)

        print(
            f"[RUN] {symbol} {index_type} "
            f"from {start_date or dates[0]} to {end_date or dates[-1]}"
        )

        for d in dates:
            if start_date and d < start_date:
                continue
            if end_date and d > end_date:
                continue

            try:
                run_single_day_vix(
                    symbol=symbol,
                    trade_date=d,
                    index_type=index_type,
                )
            except Exception as e:
                print(f"[FAIL] {symbol} {d}: {e}")

    finally:
        session.close()