from app.models.stock import Stock
from app.schemas.stock import DailyPriceCreate, StockCreate
from sqlalchemy.orm import Session


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
