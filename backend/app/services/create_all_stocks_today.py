from app.crud.stock import create_stock
from app.db.session import SessionLocal
from app.models.stock import Market
from app.schemas.stock import StockCreate
from pykrx import stock

# Create database session
db = SessionLocal()

try:
    kospi_ticker_list = stock.get_market_ticker_list(market="KOSPI")

    for ticker in kospi_ticker_list:
        name = stock.get_market_ticker_name(ticker)
        new_stock = create_stock(
            db, StockCreate(name=name, market=Market.KOSPI, ticker=ticker)
        )

    kosdaq_ticker_list = stock.get_market_ticker_list(market="KOSDAQ")

    for ticker in kosdaq_ticker_list:
        name = stock.get_market_ticker_name(ticker)
        new_stock = create_stock(
            db, StockCreate(name=name, market=Market.KOSDAQ, ticker=ticker)
        )


finally:
    db.close()
