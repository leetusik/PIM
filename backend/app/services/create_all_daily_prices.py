import pandas as pd
from app.crud.stock import create_daily_price, get_stocks
from app.db.session import SessionLocal
from app.schemas.stock import DailyPriceCreate
from pykrx import stock

# Create database session
db = SessionLocal()

try:
    stocks = get_stocks(db)
    for s in stocks:
        ohlcv = stock.get_market_ohlcv("20240101", "20250817", s.ticker)
        ohlcv["date"] = pd.to_datetime(ohlcv.index)
        ohlcv["date"] = ohlcv["date"].dt.normalize()
        for index, row in ohlcv.iterrows():
            # print(index, row)
            create_daily_price(
                db,
                DailyPriceCreate(
                    stock_id=s.id,
                    date=row["date"].date(),
                    open=row["시가"],
                    high=row["고가"],
                    low=row["저가"],
                    close=row["종가"],
                    volume=row["거래량"],
                ),
            )

finally:
    db.close()
