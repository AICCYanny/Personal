from datetime import datetime
from sqlalchemy.orm import Session

from ..models.option_quotes import OptionQuote

def parse_and_insert_quotes(
        session: Session, 
        symbol: str,
        trade_date: str,
        cp: str,
        dte_from: int,
        dte_to: int,
        raw_json: dict
    ):
    rows = raw_json.get('data', [])
    inserted = 0

    for item in rows:
        try:
            bid = item.get('Bid')
            ask = item.get('Ask')
             
            if bid is not None and ask is not None:
               mid = (bid + ask) / 2
            else:
                mid = item.get('price')

            session.add(
                OptionQuote(
                    symbol=symbol,
                    trade_date=datetime.strptime(trade_date, '%Y-%m-%d'),

                    cp=item.get('call_put'),
                    dte_from=dte_from,
                    dte_to=dte_to,

                    expiry=datetime.strptime(item['expiration_date'], '%Y-%m-%d'),
                    strike=item.get('price_strike'),
                    
                    bid=bid,
                    ask=ask,
                    mid=mid,

                    iv=item.get('iv'),
                    delta=item.get('delta'),
                    gamma=item.get('gamma'),
                    vega=item.get('vega'),
                    theta=item.get('theta'),

                    volume=item.get('volume'),
                    open_interest=item.get('openinterest'),
                )
            )
            
            inserted += 1
            
        except Exception as e:
           print(f'[WARNING] skip item: {e}')

    session.commit()
    return inserted