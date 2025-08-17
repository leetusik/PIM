from app.models.stock import DailyPrice, Stock
from app.schemas.stock import DailyPriceCreate, StockCreate
from sqlalchemy.orm import Session


def get_stocks(db: Session):
    return db.query(Stock).all()


def create_stock(db: Session, stock: StockCreate):
    db_stock = Stock(
        name=stock.name,
        market=stock.market,
        ticker=stock.ticker,
        # dart_code=stock.dart_code,
    )
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)

    return db_stock


def create_daily_price(db: Session, daily_price: DailyPriceCreate):
    db_daily_price = DailyPrice(
        stock_id=daily_price.stock_id,
        date=daily_price.date,
        open=daily_price.open,
        high=daily_price.high,
        low=daily_price.low,
        close=daily_price.close,
        volume=daily_price.volume,
    )
    db.add(db_daily_price)
    db.commit()
    db.refresh(db_daily_price)

    return db_daily_price
